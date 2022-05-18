''' gvcf_to_vcf_batch.py

Run hail batch convert gvcfs to vcfs.

'''

''' gvcf_to_vcf_batch.py 

Using gvcftools, convert gVCFs to VCFs.

'''

import hailtop.batch as hb

SAMPLES = ['HG00096', 'HG00268', 'HG00410', 'HG00419', 'HG00759', 'HG01112', 'HG01393', 'HG01500', 'HG01565', 'HG01583']

def main(): 
    backend = hb.ServiceBackend(billing_project='miah-trial', remote_tmpdir='hail-az://liricalsaaus/tmp/miah')
    b = hb.Batch(backend=backend, name="gvcf_to_vcf")
    for sample in SAMPLES:
        j = b.new_job(name=f"gvcf-to-vcf for {sample}")
        j.image("liricalacraus.azurecr.io/lirical/gvcf-to-vcf:latest")
        j.storage("16G")
        j.memory("highmem")
        gvcf_file = b.read_input(f'hail-az://liricalsaaus/wgs-1000g/{sample}/WGSData_{sample}.g.vcf.gz')
        j.command(f'gzip -dc {gvcf_file} | extract_variants | bgzip -c > {j.vcf_file}')
        b.write_output(j.vcf_file, f'hail-az://liricalsaaus/wgs-1000g/{sample}/WGSData_{sample}.vcf.gz')
    b.run(wait=False)

if __name__ == "__main__":
    main()