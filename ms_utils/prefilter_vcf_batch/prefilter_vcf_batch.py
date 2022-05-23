''' prefilter_vcf_batch.py 

Using slivar, filter variants with a gnomAD allele frequency of less than or equal to 0.01.

'''

import hailtop.batch as hb

SAMPLES = ['HG00096', 'HG00268', 'HG00410', 'HG00419', 'HG00759', 'HG01112', 'HG01393', 'HG01500', 'HG01565', 'HG01583']

def main(): 
    backend = hb.ServiceBackend(billing_project='miah-trial', remote_tmpdir='hail-az://liricalsaaus/tmp/miah')
    b = hb.Batch(backend=backend, name="prefilter_vcf")

    for sample in SAMPLES:

        vcf_path = f'hail-az://liricalsaaus/wgs-1000g/{sample}/WGSData_{sample}.vcf.gz'

        j = b.new_job(name=f"prefilter VCF for {sample}")
        j.image("liricalacraus.azurecr.io/lirical/prefilter_vcf:latest")
        j.storage("16Gi")
        input_vcf = b.read_input(vcf_path)
        input_gnomad_ref = b.read_input_group(**{'fix.zip': 'hail-az://liricalsaaus/util/gnomad.hg38.genomes.v3.fix.zip'})
        j.declare_resource_group(output={'gz': '{root}.vcf.gz', 'tbi': '{root}.vcf.gz.tbi'})

        j.command("cd /io/")
        j.command("set -ex")
        j.command(f"zcat {input_vcf} | grep -v :NA: > without_NA.vcf")
        j.command(f"java -jar /gatk.jar FixVcfHeader -I without_NA.vcf -O fixed_header.vcf")

        # Because of how the VCFs were generated, slivar is outputting 100s of MB to stdout and stderr. 
        # This is bogging Hail Batch, so redirect to a file.
        j.command(f"/slivar expr "
            f"--js /slivar-functions.js "
            f"-g {input_gnomad_ref['fix.zip']} "
            f"--info 'INFO.gnomad_popmax_af < 0.01' "
            f"--vcf fixed_header.vcf "
            f"-o {j.vcf} > {j.slivar_out} 2>&1"
        )

        j.command(f"bgzip -c {j.vcf} > {j.output.gz}")
        j.command(f"tabix {j.output.gz}")

        b.write_output(j.output, f'hail-az://liricalsaaus/wgs-1000g/{sample}/WGSDATA_{sample}.filtered.vcf')
    b.run(wait=False)

if __name__ == "__main__":
    main()