"""To reduce the cost of converting large matrix tables (500+ samples) to single-sample VCF(s) this script first runs
hl.experimental.export_entries_by_col(..) on Dataproc to convert the matrix table to single-sample .tsv files.

This script needs to run on Dataproc. Example command:

submit_to_hail_cluster bw2 single_sample_vcf_pipeline_for_large_mt.py \
    --compute-info-field \
    -o gs://seqr-bw/single_sample_vcfs/RDG_WGS_Broad_Internal \
    gs://seqr-datasets/v02/GRCh38/RDG_WGS_Broad_Internal/v26/RDG_WGS_Broad_Internal.mt
"""

import argparse
import hail as hl
import os
import re

hl.init(idempotent=True)

p = argparse.ArgumentParser()
p.add_argument("-o", "--output-dir", help="Output directory. The single-sample TSVs will be written into a "
                                          "sub-directory called 'single_sample_tsvs'.")
p.add_argument("--compute-info-field", help="Compute a VCF info field based on annotations added to matrix tables by "
                                            "the seqr loading pipeline.", action="store_true")
p.add_argument("matrix_table", help="Path of matrix table to convert to single-sample VCFs")
args = p.parse_args()

# "gs://seqr-datasets/v02/GRCh38/RDG_WGS_Broad_Internal/v28/RDG_WGS_Broad_Internal.mt"
# "gs://seqr-bw/single_sample_vcfs/RDG_WGS_Broad_Internal"

mt = hl.read_matrix_table(args.matrix_table)
existing_vcfs = [x["path"] for x in hl.hadoop_ls(os.path.join(args.output_dir, "*.vcf.bgz"))]

sample_ids_of_existing_vcfs = [
    re.sub(".vcf.bgz$", "", os.path.basename(existing_vcf)) for existing_vcf in existing_vcfs
]

if sample_ids_of_existing_vcfs:
    num_cols_before = mt.count_cols()
    mt = mt.filter_cols(hl.set(sample_ids_of_existing_vcfs).contains(mt.s), keep=False)
    num_cols_after = mt.count_cols()
    print(f"Found {len(existing_vcfs)} existing vcfs. Filtered out {num_cols_before - num_cols_after} columns.")

if args.compute_info_field:
    mt = mt.annotate_rows(info=hl.struct(
        cohort_AC=mt.AC,
        cohort_AF=hl.format("%.3f", mt.AF),
        cohort_AN=mt.AN,
        hgmd_class=mt.hgmd['class'],
        clinvar_allele_id=mt.clinvar.allele_id,
        clinvar_clinsig=mt.clinvar.clinical_significance,
        clinvar_gold_stars=mt.clinvar.gold_stars,
        consequence=mt.mainTranscript.major_consequence,
        gene_id=mt.mainTranscript.gene_id,
        gene=mt.mainTranscript.gene_symbol,
        CADD=hl.format("%.3f", mt.cadd.PHRED),
        eigen=hl.format("%.3f", mt.eigen.Eigen_phred),
        revel=hl.format("%.3f", hl.float(mt.dbnsfp.REVEL_score)),
        splice_ai=mt.splice_ai.delta_score,
        primate_ai=mt.primate_ai.score,
        exac_AF=hl.format("%.3f", mt.exac.AF_POPMAX),
        gnomad_exomes_AF=hl.format("%.3f", mt.gnomad_exomes.AF_POPMAX_OR_GLOBAL),
        gnomad_genomes_AF=hl.format("%.3f", mt.gnomad_genomes.AF_POPMAX_OR_GLOBAL),
        topmed_AF=hl.format("%.3f", mt.topmed.AF),
    ))

mt = mt.key_rows_by().select_rows('locus', 'alleles', 'filters', 'rsid', 'info')
hl.experimental.export_entries_by_col(mt, os.path.join(args.output_dir, "single_sample_tsvs"))
