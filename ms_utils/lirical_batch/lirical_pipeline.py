import json
import os
import re
from typing import Any, List, Tuple

import pandas as pd
from azure.identity import AzureCliCredential, DefaultAzureCredential
from azure.storage.blob import BlobClient, ContainerClient
import hail as hl
#import hailtop.batch as hb
from step_pipeline import pipeline, Backend, Localize, Delocalize
import configargparse
from functools import partial

DOCKER_IMAGE = "weisburd/lirical@sha256:8f056f67153e4d873c27508fb9effda9c8fa0a1f2dc87777a58266fed4f8c82b"

def define_args(pipeline) -> configargparse.ArgParser:
    """Define command-line args for the LIRICAL pipeline.

    Args:
        pipeline (step_pipeline._Pipeline): The step_pipeline pipeline object.
    """
    parser = pipeline.get_config_arg_parser()

    grp = parser.add_argument_group("LIRICAL")
    grp.add_argument("-d", "--lirical-data-dir",
                     help="Cloud Storage path of the LIRICAL reference data directory generated by the LIRICAL "
                          "download command. Prefix google storage with gs://, Azure storage with hail-az://",
                     default="gs://lirical-reference-data/LIRICAL/data")
    grp.add_argument("-e", "--exomiser-data-dir",
                     help="Cloud Storage path of the Exomiser reference data directory required by LIRICAL. "
                          "Prefix google storage with gs://, Azure storage with hail-az://",
                     default="gs://lirical-reference-data/exomiser-cli-13.0.0/2109_hg38")
    grp.add_argument("-o", "--output-dir",
                     help="Cloud Storage directory where to write LIRICAL output. "
                        "Prefix google storage with gs://, Azure storage with hail-az://",
                     required=True)

    thresholds_grp = grp.add_mutually_exclusive_group()
    thresholds_grp.add_argument("-m", "--mindiff",
                                help="Minimum number of differential diagnoses to show in the HTML output, regardless "
                                     "of their post-test probability",
                                type=int)
    thresholds_grp.add_argument("-t", "--threshold",
                                help="Post-test probability threshold as a percentage. Diagnoses with a post-test "
                                     "probability above this threshold will be included in the HTML output.",
                                type=float)

    grp.add_argument("--transcriptdb",
                     help="Which transcript models to use",
                     choices={"UCSC", "Ensembl", "RefSeq"})
    grp.add_argument("--orphanet",
                     help="Use annotation data from Orphanet",
                     action="store_true")
    grp.add_argument("-g", "--use-global",
                     help="Run LIRICAL with the --global flag",
                     action="store_true")

    grp.add_argument("--vcf",
                     help="Cloud Storage path that contains single-sample VCFs referenced by the phenopackets. More "
                          "than one path can be provided by specifying this argument more than once. Also each path "
                          "can optionally contain wildcards (*).",
                     required=True,
                     action="append")
    grp.add_argument("phenopacket_paths",
                     nargs="+",
                     help="Cloud Storage path of Phenopacket JSON files to process. More than one path can be "
                          "specified. Also each path can optionally contain wildcards (*).")

    grp.add_argument("-s", "--sample-id", help="Optionally, process only this sample id. Useful for testing.")

    return parser

def _az_parse_blob_path(path: str) -> Tuple[str, str, str]:
    path_fmt = re.sub("^hail-az://", "", path).rstrip("/")
    sep_count = path_fmt.count("/")
    if sep_count == 1:
        account, container_name = path_fmt.split('/')
        return (account, container_name, None)
    elif sep_count > 1:
        return path_fmt.split('/', 2)
    else:
        # TODO, something more sensible here.
        raise Exception("Azure storage path provided must be of the format 'hail-az://ACCOUNT/CONTAINER[/BLOB]")

def _az_account_name_to_blob_service_url(account_name: str) -> str:
    return f"https://{account_name}.blob.core.windows.net"

def az_storage_read(path: str, credential: Any = None) -> str:
    if not credential:
        # Fallback in the case that the caller hasn't already generated a credential.
        credential = DefaultAzureCredential()

    # TODO alternatively reformat the Hail blob path to a standard URI and access the blob that way.
    account_name, container_name, blob_name = _az_parse_blob_path(path)
    account_url = _az_account_name_to_blob_service_url(account_name)
    return BlobClient(account_url=account_url, 
                      container_name=container_name, 
                      blob_name=blob_name, 
                      credential=credential).download_blob().content_as_text()

def az_storage_ls(path: str, credential: Any = None) -> List[str]:
    """
    If path is a blob, returns a list with one element. If path is a blob prefix (i.e., directory), returns an element for each
    blob contained directly under that prefix. Does not search recursively.
    """

    if not credential:
        # Fallback in the case that the caller hasn't already generated a credential.
        credential = DefaultAzureCredential()

    # TODO, consider using datalake gen 2 and ASFS for Hadoop file system API access.
    account_name, container_name, blob_name = _az_parse_blob_path(path)
    account_url = _az_account_name_to_blob_service_url(account_name)
    container_client = ContainerClient(account_url=account_url, container_name=container_name, credential=credential) 
    blob_list = container_client.list_blobs(name_starts_with=blob_name)

    # Filter to only top-level blobs and return.
    if blob_name:
        prefix_len = len(blob_name) + 1
    else:
        prefix_len = 0

    return [f"hail-az://{account_name}/{container_name}/{b.name}" for b in blob_list if "/" not in b.name[prefix_len:]]

def gcs_storage_read(path: str) -> str:
    with hl.hadoop_open(path, "r") as f:
        contents = f.read()    
    return contents

def gcs_storage_ls(path: str) -> List[str]:
    return [r["path"] for r in hl.hadoop_ls(path)]

def parse_args(pipeline):
    """Define and parse command-line args.

    Args:
        pipeline (step_pipeline._Pipeline): The step_pipeline pipeline object.

    Return:
         argparse.Namespace: parsed command-line args
         pandas.DataFrame: DataFrame with 1 row per phenopacket and columns: "sample_id", "phenopacket_path", "vcf_path"
    """

    define_args(pipeline)
    args = pipeline.parse_args()

    parser = pipeline.get_config_arg_parser()

    # Determine which cloud we're working against. In theory, Hail Batch would be agnostic to this and could work
    # across clouds, but that could potentially generate unexpected exgress charges, so we'll rule it out for the time being.
    def get_storage_prefix(path):
        return path.split(":")[0]

    all_storage_paths = [args.lirical_data_dir, args.exomiser_data_dir, args.output_dir] + args.phenopacket_paths + args.vcf
    all_storage_prefixes = {get_storage_prefix(path) for path in all_storage_paths}
    if len(all_storage_prefixes) != 1:
        print(all_storage_prefixes)
        parser.error("All provided storage paths have a common prefix, either gs:// or hail-az://")
    storage_prefix = all_storage_prefixes.pop()
    if storage_prefix not in ["gs", "hail-az"]:
        parser.error(f"Unknown storage prefix used {storage_prefix}")
    
    # Cloud specific configuration.
    if storage_prefix == 'gs':
        storage_read = gcs_storage_read
        storage_ls = gcs_storage_ls

        # initialize hail with workaround for Hadoop bug involving requester-pays buckets:
        # https://discuss.hail.is/t/im-encountering-bucket-is-a-requester-pays-bucket-but-no-user-project-provided/2536/2
        def get_bucket(path):
            return path.split("/")[2]

        all_buckets = {
            get_bucket(path) for path in [args.lirical_data_dir, args.exomiser_data_dir] + args.phenopacket_paths + args.vcf
        }

        hl.init(log="/dev/null", quiet=True, idempotent=True, spark_conf={
            "spark.hadoop.fs.gs.requester.pays.mode": "CUSTOM",
            "spark.hadoop.fs.gs.requester.pays.buckets": ",".join(all_buckets),
            "spark.hadoop.fs.gs.requester.pays.project.id": args.gcloud_project,
        })
    elif storage_prefix == 'hail-az':
        # Initialize the Azure Credential once here to speed up multiple calls to the Azure Python SDK.

        # Running this script requires that the user be logged in via Azure CLI. The Python SDK will access these credentials
        # and perform requested operations using the identity of the currently logged in user.
        azure_credential = AzureCliCredential()

        storage_read = partial(az_storage_read, credential=azure_credential)
        storage_ls = partial(az_storage_ls, credential=azure_credential)

    # validate input paths
    def check_paths(paths):
        checked_paths = []
        for path in paths:
            current_paths = storage_ls(path)
            if not current_paths:
                parser.error(f"{path} not found")
            checked_paths += current_paths
        return checked_paths

    phenopacket_paths = check_paths(args.phenopacket_paths)
    vcf_paths = check_paths(args.vcf)

    # create DataFrame of phenopackets to process, with columns: "sample_id", "phenopacket_path", "vcf_path"
    rows = []
    requested_sample_id_found = False
    print(f"Processing {len(phenopacket_paths)} phenopacket(s)")
    for phenopacket_path in phenopacket_paths:
        print(f"Parsing {phenopacket_path}")
        phenopacket_json = json.loads(storage_read(phenopacket_path))
        sample_id = phenopacket_json.get("subject", {}).get("id")
        if args.sample_id:
            if args.sample_id != sample_id:
                continue
            else:
                requested_sample_id_found = True

        if sample_id is None:
            parser.error(f"{phenopacket_path} is missing a 'subject' section")

        if ("htsFiles" not in phenopacket_json or not isinstance(phenopacket_json["htsFiles"], list) or
                "uri" not in phenopacket_json["htsFiles"][0]):
            parser.error(f"{phenopacket_path} is missing an 'htsFiles' section with a VCF uri")
        vcf_filename = phenopacket_json["htsFiles"][0]["uri"].replace("file:///", "")

        matching_vcf_paths = [vcf_path for vcf_path in vcf_paths if vcf_filename in vcf_path]
        if not matching_vcf_paths:
            parser.error(f"Couldn't find {vcf_filename} referred to by {phenopacket_path}")
        vcf_path = matching_vcf_paths[0]

        rows.append({
            "sample_id": sample_id,
            "phenopacket_path": phenopacket_path,
            "vcf_path": vcf_path,
        })
        if requested_sample_id_found:
            break

    metadata_df = pd.DataFrame(rows)

    return args, metadata_df

def main():
    bp = pipeline("LIRICAL", backend=Backend.HAIL_BATCH_SERVICE, config_file_path="~/.step_pipeline")
    args, metadata_df = parse_args(bp)

    for _, row in metadata_df.iterrows():
        s1 = bp.new_step(f"LIRICAL: {row.sample_id}", image=DOCKER_IMAGE, cpu=2, storage="70Gi", memory="highmem",
                         localize_by=Localize.COPY, delocalize_by=Delocalize.COPY)

        #s1.switch_gcloud_auth_to_user_account()
        phenopacket_input = s1.input(row.phenopacket_path)
        vcf_input = s1.input(row.vcf_path)
        lirical_data_dir_input = s1.input(args.lirical_data_dir)
        exomiser_data_dir_input = s1.input(args.exomiser_data_dir)

        s1.command("cd /io/")
        s1.command("set -ex")
        s1.command(f"ln -s {lirical_data_dir_input} data")

        # the vcf's path within the container needs to match the vcf path specified in the phenopacket
        if row.vcf_path.endswith("gz"):
            unzipped_vcf_path = re.sub("(.bgz|.gz)$", "", os.path.basename(vcf_input.local_path))
            # filter out ":NA:" fields to work around a bug where DP="NA" in some VCF rows.
            s1.command(f"gunzip -c {vcf_input} | grep -v :NA: > /{unzipped_vcf_path}")
        else:
            s1.command(f"ln -s {vcf_input} /{vcf_input.filename}")

        lirical_command = f"java -jar /LIRICAL.jar P -p {phenopacket_input} -e {exomiser_data_dir_input} --tsv"
        if args.use_global:
            lirical_command += " --global"
        if args.orphanet:
            lirical_command += " --orphanet"
        if args.threshold is not None:
            lirical_command += f" --threshold {args.threshold}"
        if args.mindiff is not None:
            lirical_command += f" --mindiff {args.mindiff}"
        if args.transcriptdb:
            lirical_command += f" --transcriptdb {args.transcriptdb}"
        s1.command(lirical_command)

        #output_path_prefix = os.path.join(args.output_dir, f"{row.sample_id}.lirical")
        phenopacket_input_prefix = re.sub("(.phenopacket)?.json$", "", phenopacket_input.filename)
        output_path_prefix = os.path.join(args.output_dir, f"{phenopacket_input_prefix}.lirical")

        s1.output("lirical.html", output_path=f"{output_path_prefix}.html")
        s1.output("lirical.tsv", output_path=f"{output_path_prefix}.tsv")

    # run the pipeline
    bp.run()

if __name__ == "__main__":
    main()
