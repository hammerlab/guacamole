package org.hammerlab.guacamole.data

import org.hammerlab.genomics.readsets.args.path.{ PathPrefix, UnprefixedPath }
import org.hammerlab.test.resources.File

object NA12878 {
  implicit def prefixOpt = Some(PathPrefix(File("illumina-platinum-na12878")))

  // See illumina-platinum-na12878/run_other_callers.readme.sh for how these files were generated
  val subsetBam = UnprefixedPath("NA12878.10k_variants.plus_chr1_3M-3.1M.bam")
  val expectedCallsVCF = UnprefixedPath("NA12878.subset.vcf")
  val chr1PrefixFasta = UnprefixedPath("chr1.prefix.fa")
}
