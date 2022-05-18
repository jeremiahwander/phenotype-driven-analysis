''' populate_sa_1000g.py 

Starting with gVCFs from standard secondary analysis pipelines, 
move a copy to a secondary, working storage account.

- Assumes the existence of four environment variables 
  - SRC_TOKEN and DEST_TOKEN containing SAS tokens for storage account access.
  - SRC_SA and DEST_SA corresponding storage account names.

- Assumes that the utility AZCOPY has been installed and can be found on the path.

'''

import os
import re
import subprocess

SRC_TOKEN = os.getenv('SRC_TOKEN')
DEST_TOKEN = os.getenv('DEST_TOKEN')

SRC_SA = os.getenv('SRC_SA')
DEST_SA = os.getenv('DEST_SA')
SRC_GVCF_URLS = [  
  f"https://{SRC_SA}.blob.core.windows.net/cromwell-executions/WholeGenomeGermlineSingleSample/7a979d1f-1e5e-4136-8358-ce1e7af032f3/call-BamToGvcf/VariantCalling/e3f192a9-f1d5-4596-9035-2bf68d72127b/call-MergeVCFs/execution/WGSData_HG00096.g.vcf.gz",
  f"https://{SRC_SA}.blob.core.windows.net/cromwell-executions/WholeGenomeGermlineSingleSample/204183c3-2d23-42e0-89c8-e00e32b5805f/call-BamToGvcf/VariantCalling/2f933ca2-51c1-4d36-ad5e-59972c6485da/call-MergeVCFs/execution/WGSData_HG00268.g.vcf.gz",
  f"https://{SRC_SA}.blob.core.windows.net/cromwell-executions/WholeGenomeGermlineSingleSample/852e0188-bb21-4f2f-8f06-8205f40b40eb/call-BamToGvcf/VariantCalling/aff71f5f-c6d7-4a08-b16e-997f8713779a/call-MergeVCFs/execution/WGSData_HG00410.g.vcf.gz",
  f"https://{SRC_SA}.blob.core.windows.net/cromwell-executions/WholeGenomeGermlineSingleSample/9457e560-d6bc-4377-afe6-4e42069f9193/call-BamToGvcf/VariantCalling/0c3a19c4-594d-4089-9e69-5346e72cdd10/call-MergeVCFs/execution/WGSData_HG00419.g.vcf.gz",
  f"https://{SRC_SA}.blob.core.windows.net/cromwell-executions/WholeGenomeGermlineSingleSample/22932a6f-8280-4b79-826c-0a11ac807cd4/call-BamToGvcf/VariantCalling/16fd06b6-bd64-40ce-983d-6d29d07fb992/call-MergeVCFs/execution/WGSData_HG00759.g.vcf.gz",
  f"https://{SRC_SA}.blob.core.windows.net/cromwell-executions/WholeGenomeGermlineSingleSample/f9b4f692-a1f6-471e-b5f5-073c9f76292a/call-BamToGvcf/VariantCalling/1e6486d3-03d8-4990-bb7a-345a64dad054/call-MergeVCFs/execution/WGSData_HG01112.g.vcf.gz",
  f"https://{SRC_SA}.blob.core.windows.net/cromwell-executions/WholeGenomeGermlineSingleSample/5c7f5891-ed83-4293-9302-60941b35b057/call-BamToGvcf/VariantCalling/23268e3d-163a-465d-8de6-cc9ccc48384a/call-MergeVCFs/execution/WGSData_HG01393.g.vcf.gz",
  f"https://{SRC_SA}.blob.core.windows.net/cromwell-executions/WholeGenomeGermlineSingleSample/ebdca2a2-57a9-4a44-85f9-e33001e89e30/call-BamToGvcf/VariantCalling/73531789-d55a-41ec-afcb-484f336bb27f/call-MergeVCFs/execution/WGSData_HG01500.g.vcf.gz",
  f"https://{SRC_SA}.blob.core.windows.net/cromwell-executions/WholeGenomeGermlineSingleSample/4d73cbb7-616b-44e8-859b-e150310f025f/call-BamToGvcf/VariantCalling/b1c84e5d-5b04-440a-a975-a8aed9d5d164/call-MergeVCFs/execution/WGSData_HG01565.g.vcf.gz",
  f"https://{SRC_SA}.blob.core.windows.net/cromwell-executions/WholeGenomeGermlineSingleSample/675c1bfd-b3bf-4a73-b569-db450acafbbd/call-BamToGvcf/VariantCalling/7e5550a4-cc07-4bd6-8ad6-6d79bb64e110/call-MergeVCFs/execution/WGSData_HG01583.g.vcf.gz",
]

DEST_CONTAINER = "wgs-1000g"

RE_1000G_SID = re.compile('HG\d+')

def sid_from_gvcf_blob_name(blob_name: str) -> str:
    if result := RE_1000G_SID.search(blob_name):
        return result.group()
    return None

if not SRC_TOKEN or not DEST_TOKEN:
    raise ValueError('environment variables SRC_TOKEN and DEST_TOKEN must be specified.')

# TODO AZ check

for src_url in SRC_GVCF_URLS:

    # Formualte the complete src url.
    src_url_with_token = f"{src_url}?{SRC_TOKEN}"

    # Formulate the complete destination URL.
    blob_name = src_url.split('/')[-1]
    sid = sid_from_gvcf_blob_name(blob_name)

    dest_url = f"https://{DEST_SA}.blob.core.windows.net/{DEST_CONTAINER}/{sid}/{blob_name}"
    dest_url_with_token = f"{dest_url}?{DEST_TOKEN}"

    # Call azcopy.
    subprocess.run(["azcopy", "cp", f"{src_url_with_token}", f"{dest_url_with_token}"])
    #print(f"azcopy cp {src_url_with_token} {dest_url_with_token}")



