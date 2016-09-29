#!/usr/bin/env bash

# Commands used to run other germline variant callers on the illumina platinum
# subset test data. Run this from the root of the guacamole repository.

if [ -z "$GATK_JAR" -o -z "$REFERENCE" ]; then
  echo "Set \$GATK_JAR and \$REFERENCE" >&2
  exit 1
fi

# Commands to generate index / dict for reference if needed:
# samtools faidx "$REFERENCE"
# java -jar "$PICARD_JAR" CreateSequenceDictionary \
#    REFERENCE="$REFERENCE" \
#    "OUTPUT=${REFERENCE}.dict"

# Unified genotyper
time java -jar "$GATK_JAR" \
    -T UnifiedGenotyper \
    -R "$REFERENCE" \
    -I src/test/resources/illumina-platinum-na12878/NA12878.10k_variants.plus_chr1_3M-3.1M.bam \
    -L chr1:1-6700000 \
    -glm BOTH \
    -o src/test/resources/illumina-platinum-na12878/unified_genotyper.vcf

# Haplotype caller
time java -jar "$GATK_JAR" \
    -T HaplotypeCaller \
    -R "$REFERENCE" \
    -I src/test/resources/illumina-platinum-na12878/NA12878.10k_variants.plus_chr1_3M-3.1M.bam \
    -L chr1:1-6700000 \
    --genotyping_mode DISCOVERY \
    -stand_emit_conf 10 \
    -stand_call_conf 30 \
    -o src/test/resources/illumina-platinum-na12878/haplotype_caller.vcf

# germline-standard
time scripts/guacamole germline-standard \
    --reads src/test/resources/illumina-platinum-na12878/NA12878.10k_variants.plus_chr1_3M-3.1M.bam \
    --loci chr1:0-6700000 \
    --out src/test/resources/illumina-platinum-na12878/germline_standrd.dir.vcf
