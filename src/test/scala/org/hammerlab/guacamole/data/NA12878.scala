package org.hammerlab.guacamole.data

import org.apache.hadoop.fs.Path
import org.hammerlab.test.resources.File

object NA12878 {
  // See illumina-platinum-na12878/run_other_callers.readme.sh for how these files were generated
  val subsetBam = new Path(File("illumina-platinum-na12878/NA12878.10k_variants.plus_chr1_3M-3.1M.bam"))
  val expectedCallsVCF = File("illumina-platinum-na12878/NA12878.subset.vcf")
  val chr1PrefixFasta = File("illumina-platinum-na12878/chr1.prefix.fa")
}
