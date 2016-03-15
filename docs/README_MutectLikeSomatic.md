# Mutation Detector
## Algorithmic flow
2. Iterate over normal/tumor pileups. 
3. Exclude sites that are present in the "Panel of Normal" `VCF` file, unless they are also present
    in the Cosmic `VCF` file.
    * Note that the "Panel of Normal" `VCF` file is created by running a bunch of normals as the tumor, and not
    supplying a tumor sample to filer on. We used the public MuTect to generate this panel of normal list of false
    positive calls.
3. Exclude non-ACGT sites in the reference.
4. Get the ratio of MAPQ 0 reads in the tumor and seperately in the normal sample,
    if the ratio is greater than `0.5` in either, discard the site.
5. Discard any reads from the tumor and normal pileup that:
    2. Are MAPQ0
    3. Are Base Quality less than 5.
5. Discard any reads from the Tumor pileup that:
    1. have more than 30% of bases soft clipped
    4. Have a total sum of mismatch quality scores over 100 (this includes the allele in the
        current pileup column)
    5. When two reads align to the same pileup column, discard the read with the lowest base quality
    6. Any reads with the `XT=M` tag set (BWA-aligner's flag for sensitive remapping of a read's mate)
6. If there are less than 3 reads left after the above filters in the Tumor, don't bother proceeding.
7. If more than `0.3` of the reads are removed due to presence of the `XT=M` tag, maximum quality score of 
    mismatches being surpassed, or the maximum number of soft-clipped bases, discard the position (the previous set of
    stringent tumor filters).
8. In the normal pileup:
    calculate the LOD score that a read is at allele frequency of the contamination level (a number near 0)
    vs at 0.5. Calculate this as described in the MuTect paper with DBSNP site specific settings if the
    position is on a DBSNP site. This is in the classification as Somatic section of the paper.
9. In the tumor pileup:
    Calculate the LOD score that the allele is at whatever the observed frequency is, 
    vs the contamination level (same non-0 value used in Normal above). Compare this to the tumor LOD cutoff described
    in the paper.
10. If more than 1 alternate allele passes the LOD score cutoff in the tumor, discard the site
11. If the identified mutant allele frequency is less than 0.5% (0.005) discard the site.
12. If the normal allele has 3% or more reads, or 2 reads matching the alt, then also check that the sum of alternate allele base quality scores is less than 20
13. Get the maximum MAPQ score of a read supporting the alternate allele in the tumor, if that score is less than 
    20, discard the read.
14. Discard sites with sufficient power to detect the mutation on both strands when the mutation is only detected on one strand.
    14. Calculate the power to detect mutations seperately on the positive and negative strand of the tumor as described
        in the paper. Briefly:
        1. get the minimum
        number of reads to trigger the stranded theta_tumor cutoff for the lod score (2.0) assuming a per-read error rate
        of 0.001, and the original observed alternate allele frequency in both strands of the pileup.
        2. get the probability that the number of reads sampled given the tumor strand specific depth is at least k, this
        is the sum of binomial probabilities from k=min_k..depth.
    15. Calculate the strand specific LOD, assuming the observed strand-specific alternate allele frequency.
    16. If the strand-specific LOD score is less than 2, and the power to detect on that strand is at least 0.9 discard the
        read. Do the same to both strands.
    17. Get the number of reads supporting insertions or deletions within 5bp of the pileup column on either side. For insertions
        be sure to check both the start and end position of the insertion relative to the offset. For deletions you only need
        to check the insertion start position's offset. If more than 3 insertions, or more than 3 deletions are observed,
        discard the position.
18. Determine if there is a location specific bias in the mutation position within the read positions.
        Does it only happen at the beginning or ends of reads? For this analyses use the raw tumor data and
        only apply the base and mapq filtering. Do not use the fully filtered tumor data for this.
    1. Get the reads supporting just the alternate allele on both the positive and negative strands separately.
    2. If at least than 1 positive allele-supporting strand read exists, get:
        1. the median offset of the pileup column for that allele, the median needs to be more than 10.
        2. the median absolute deviation of the offsets for that allele, the MAD needs to be more than 3.
    3. Repeat for the strand specific things for the other strand. Discard if either strand fails.

### Differences from Broad implementation.
1. For the INDEL filter, Broad's MuTect has a minor bug where it only looks at the start position of insertions. If a 
    mutation is observed even a single base position away from the end position of a long insertion, it would be missed
    by the original implementation, but it will be noticed by ours.
2. Broad's implementation discards a site if the fraction of tumor reads filtered due to Soft-clipping exceeds 0.3, our
    method is more stringent and will fail if any combination of reads filtered due to the following three criteria 
    are over 0.3.
    1. soft-clipping
    2. `XT=M` 
    3. the sum of mismatching allele quality scores is over 100

