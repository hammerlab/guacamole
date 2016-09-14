package org.hammerlab.guacamole.readsets.args

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.kohsuke.args4j.{Option => Args4jOption}

trait ReferenceFastaArgs {
  @Args4jOption(name = "--reference-fasta", required = true, usage = "Local path to a reference FASTA file")
  var referenceFastaPath: String = null

  @Args4jOption(name = "--reference-fasta-is-partial", usage = "Treat the reference fasta as a partial reference")
  var referenceFastaIsPartial: Boolean = false

  def reference(sc: SparkContext) = ReferenceBroadcast(referenceFastaPath, sc, partialFasta = referenceFastaIsPartial)
}
