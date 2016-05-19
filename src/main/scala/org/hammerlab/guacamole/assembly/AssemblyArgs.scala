package org.hammerlab.guacamole.assembly

import org.bdgenomics.utils.cli.Args4jBase
import org.kohsuke.args4j.{Option => Args4jOption}

trait AssemblyArgs extends Args4jBase {
  @Args4jOption(name = "--kmer-size", usage = "Length of kmer used for DeBruijn Graph assembly")
  var kmerSize: Int = 45

  @Args4jOption(name = "--assembly-window-range", usage = "Number of bases before and after to check for additional matches or deletions")
  var assemblyWindowRange: Int = 20

  @Args4jOption(name = "--min-occurrence", required = false, usage = "Minimum occurrences to include a kmer ")
  var minOccurrence: Int = 3

  @Args4jOption(name = "--min-area-vaf", required = false, usage = "Minimum variant allele frequency to investigate area")
  var minAreaVaf: Int = 5

  @Args4jOption(name = "--min-mean-kmer-quality", usage = "Minimum mean base quality to include a kmer")
  var minMeanKmerQuality: Int = 0

  @Args4jOption(name = "--shortcut-assembly", required = false, usage = "Skip assembly process in inactive regions")
  var shortcutAssembly: Boolean = false
}
