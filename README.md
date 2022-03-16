This repo contains pipelines and scripts for phenotype-driven analysis of rare disease cases. 

### LIRICAL

LIkelihood Ratio Interpretation of Clinical AbnormaLities (LIRICAL) is a tool 
"... that calculates the likelihood ratio of each observed or excluded phenotypic abnormality. 
If genomic data is available, likelihood ratios are additionally calculated for genotypes. 
In contrast to previous approaches based on semantic similarity, LIRICAL provides an estimate 
of the posttest probability of candidate diagnoses."
[[Robinson 2020](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7477017 )]   

#### *LIRICAL Tool Details*

*Inputs:*
* Exomiser reference data directory 
* Single-sample VCF
* Phenopacket JSON file that contains:
  * Patient phenotype as a list of HPO terms
  * Path of the single-sample VCF

*Outputs:*
* HTML file containing user-friendly descriptions and visualizations of top hits
* TSV file containing the full set of results, including ones below the post-test probability threshold.   

*Example command:*   

```
java -jar LIRICAL.jar P -p sample1.phenopacket.json -e /ref_data/exomiser-cli-13.0.0/2109_hg38
```

*Additional Info:*

* [Phenopacket docs](https://phenopacket-schema.readthedocs.io/en/latest/)
* [LIRICIAL docs](https://lirical.readthedocs.io/en/latest/index.html)

#### *docker*

`./lirical/docker` directory contains files for building a docker image with LIRICAL installed.
To build the image, run:
```
cd docker; make
```

#### *lirical_pipeline.py*

`./lirical/lirical_pipeline.py` script contains a [Hail Batch](https://hail.is/docs/batch/getting_started.html) 
pipeline for running LIRICAL in parallel on any number of phenopackets.
The script uses the [step-pipeline](https://github.com/bw2/step-pipeline) library, which is a wrapper
around Hail Batch that further simplifies some common aspects of pipeline development. 

*Requirements:*

To install required python libraries, run: `python3 -m pip install -r requirements.txt`  

*Example command:*

```
python3 lirical/lirical_pipeline.py \
  -d gs://lirical-reference-data/LIRICAL/data \
  -e gs://lirical-reference-data/exomiser-cli-13.0.0/2109_hg38 \
  --vcf gs://lirical-reference-data/example_inputs/project.NIST.hc.snps.indels.vcf \
  -o gs://your-bucket/LIRICAL_output/ \
  gs://lirical-reference-data/example_inputs/example1.phenopacket.json  
```
 

### Other Scripts

#### *single_sample_vcf_pipeline.py* 
`single_sample_vcf_pipeline.py` is a [Hail Batch](https://hail.is/docs/batch/getting_started.html) pipeline 
that takes a [Hail Matrix Table](https://hail.is/docs/0.2/hail.MatrixTable.html) and exports single sample VCF(s) from it.   


*Example command:*

```
python3 single_sample_vcf_pipeline.py gs://bucket/input.mt -s sample_id1 -s sample_id2 -o gs://output-bucket/path/
```
