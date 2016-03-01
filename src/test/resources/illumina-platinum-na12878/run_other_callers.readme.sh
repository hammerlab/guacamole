#!/bin/bash

# Commands used to run other germline variant callers on the illumina platinum
# subset test data.

# Change this to point to your GATK jar.
GATK_JAR=~/oss/gatk-binary/GenomeAnalysisTK.jar

# Unified genotyper
time java -jar $GATK_JAR \
    -T UnifiedGenotyper \
    -R src/test/resources/illumina-platinum-na12878-extract/chr1.prefix.fa \
    -I src/test/resources/illumina-platinum-na12878-extract/NA12878.10k_variants.plus_chr1_3M-3.1M.chr_fixed.bam \
    -L chr1:1-6700000 \
    -glm INDEL \
    -o src/test/resources/illumina-platinum-na12878-extract/unified_genotyper.vcf

exit

# Haplotype caller
time java -jar $GATK_JAR \
    -T HaplotypeCaller \
    -R src/test/resources/illumina-platinum-na12878-extract/chr1.prefix.fa \
    -I src/test/resources/illumina-platinum-na12878-extract/NA12878.10k_variants.plus_chr1_3M-3.1M.chr_fixed.bam \
    -L chr1:1-6700000 \
    --genotyping_mode DISCOVERY \
    -stand_emit_conf 10 \
    -stand_call_conf 30 \
    -o src/test/resources/illumina-platinum-na12878-extract/haplotype_caller.vcf

# germline-standard
time scripts/guacamole germline-standard \
    --reads src/test/resources/illumina-platinum-na12878-extract/NA12878.10k_variants.plus_chr1_3M-3.1M.chr_fixed.bam \
    --loci chr1:0-6700000 \
    --out src/test/resources/illumina-platinum-na12878-extract/germline_standrd.dir.vcf
