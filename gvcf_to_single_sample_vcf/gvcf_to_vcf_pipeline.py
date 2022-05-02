"""This Hail Batch pipeline converts gVCF(s) to VCF(s) by running the GATK GenotypeGVCFs command.
This is useful when gVCFs are easier/cheaper to access than the joint-called matrix table. One disadvantage of
converting gVCFs to VCFs using this pipeline (rather than running one of the other pipelines that generate VCFs from the
joint-called matrix table) is that the gVCFs are upstream of the VQSR and RandomForst filters, and so the output VCF
won't include variant PASS/non-PASS filters.
"""

import json
import hail as hl
import os
import re

import pandas as pd
from step_pipeline import pipeline, Backend, Localize, Delocalize

DOCKER_IMAGE = "weisburd/lirical@sha256:3acb48b5f7d833fd466579afceb79e55b59700982955e8a3f9ca45db382e042a"


def define_args(pipeline):
    """Define command-line args for the pipeline.

    Args:
        pipeline (step_pipeline._Pipeline): The step_pipeline pipeline object.
    """
    parser = pipeline.get_config_arg_parser()
    parser.add_argument("-o", "--output-path", help="Google Storage output path where to write the VCFs", required=True)
    parser.add_argument("gvcf_path",
                     nargs="+",
                     help="Google Storage path of gVCF file(s) or a text file containing one gVCF path per line")


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

    # initialize hail with workaround for Hadoop bug involving requester-pays buckets:
    # https://discuss.hail.is/t/im-encountering-bucket-is-a-requester-pays-bucket-but-no-user-project-provided/2536/2
    def get_bucket(path):
        if not path.startswith("gs://"):
            parser.error(f"{path} must start with gs://")
        return re.sub("^gs://", "", path).split("/")[0]

    all_buckets = {
        get_bucket(path) for path in [args.lirical_data_dir, args.exomiser_data_dir] + args.phenopacket_paths + args.vcf
    }

    hl.init(log="/dev/null", quiet=True, idempotent=True, spark_conf={
        "spark.hadoop.fs.gs.requester.pays.mode": "CUSTOM",
        "spark.hadoop.fs.gs.requester.pays.buckets": ",".join(all_buckets),
        "spark.hadoop.fs.gs.requester.pays.project.id": args.gcloud_project,
    })

    # validate input paths
    def check_paths(paths):
        checked_paths = []
        for path in paths:
            if not path.startswith("gs://"):
                parser.error(f"Path must start with gs:// {path}")
            current_paths = [r["path"] for r in hl.hadoop_ls(path)]
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
        with hl.hadoop_open(phenopacket_path, "r") as f:
            phenopacket_json = json.load(f)
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
    bp = pipeline("gVCF to VCF", backend=Backend.HAIL_BATCH_SERVICE, config_file_path="~/.step_pipeline")
    args, metadata_df = parse_args(bp)

    for _, row in metadata_df.iterrows():
        s1 = bp.new_step(f"LIRICAL: {row.sample_id}", image=DOCKER_IMAGE, cpu=2, storage="70Gi", memory="highmem",
                         localize_by=Localize.COPY, delocalize_by=Delocalize.COPY)

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