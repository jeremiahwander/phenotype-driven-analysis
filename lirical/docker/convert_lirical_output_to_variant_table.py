import argparse
from pprint import pprint
#import pandas as pd
import re

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--gene-id-lookup", help="Path of TSV file downloaded from the HGNC website that maps NCBI gene ids "
        "to Ensembl (ENSG) ids, to gene names. The table must have columns: ...")
    p.add_argument("--output-path", help="Output file path. Defaults to the input file path with a "
        ".variants_table.tsv.gz suffix")

    p.add_argument("lirical_tsv", help="The TSV results file generated by LIRICAL for an individual")
    args = p.parse_args()

    if not args.output_path:
        args.output_path = re.sub(".tsv$", "", args.lirical_tsv) + ".variants_table.tsv.gz"

    #df = pd.read_table(args.lirical_output_tsv)
    f = open(args.lirical_tsv, "rt")
    next(f)
    sample_id_line = next(f)
    sample_id = sample_id_line.replace("! Sample:", "").strip()


    header = None

    output_rows = []
    for line in f:
        if line.startswith("!"):
            continue
        line = line.strip()
        if header is None:
            header = line.split("\t")
            continue
        fields = line.split("\t")
        row = dict(zip(header, fields))
        rank = int(row["rank"])
        entrez_gene_id = int(re.sub("^NCBIGene:", "", row["entrezGeneId"]))
        post_test_probability = float(row["posttestprob"].strip("%"))
        for variant in row["variants"].split("; "):
            variant_locus, variant_hgvs, variant_pathogenicity_score, variant_zygosity = variant.split(" ")
            variant_zygosity = variant_zygosity.strip("[]")
            variant_pathogenicity_score = float(re.sub("pathogenicity:", "", variant_pathogenicity_score))

            variant_chrom = variant_pos = variant_ref = variant_alt = None
            match = re.match("(.+):([\d]+)([a-zA-Z]+)[>]([a-zA-Z]+)", variant_locus)
            if match:
                variant_chrom, variant_pos, variant_ref, variant_alt = match.groups()
                variant_pos = int(variant_pos)
            else:
                print(f"WARNING: unexpected variant locus format: {variant_locus}")

            output_rows.append({
                "sampleId": sample_id,
                "rank": rank,
                "entrezGeneId": entrez_gene_id,
                "disease": row["diseaseCurie"],
                "postTestProbability": post_test_probability,
                "variantLocus": variant_locus,
                "variantChrom": variant_chrom,
                "variantPos": variant_pos,
                "variantRef": variant_ref,
                "variantAlt": variant_alt,
                "variantHgvs": variant_hgvs,
                "variantPathogenicityScore": variant_pathogenicity_score,
                "variant_zygosity": variant_zygosity,
            })

        break
    pprint(output_rows)
    #pd.DataFrame(output_rows).to_tsv("")

if __name__ == "__main__":
    main()

"""
{'compositeLR': '136,267.853',
 'diseaseCurie': 'OMIM:612782',
 'diseaseName': 'Immunodeficiency 9',
 'entrezGeneId': 'NCBIGene:84876',
 'posttestprob': '94.35%',
 'pretestprob': '1/8166',
 'rank': '1',
 'variants': "...".split("; ") yields
    ['chr12:121626865GGCCCC>G', 'NM_032790.3:c.127_131del:p.(Pro46Valfs*40)', 'pathogenicity:1.0', '[HOMOZYGOUS_ALT]']
    ['chr12:121641535T>C', 'NM_032790.3:c.801C>C:p.(=)', 'pathogenicity:0.0', '[HETEROZYGOUS]']

}
"""