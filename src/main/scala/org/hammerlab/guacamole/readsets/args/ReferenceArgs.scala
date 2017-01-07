package org.hammerlab.guacamole.readsets.args

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.kohsuke.args4j.{ Option => Args4jOption }

trait HasReference {
  def referencePath: String
  def referenceIsPartial: Boolean
  def reference(sc: SparkContext) = ReferenceBroadcast(referencePath, sc, partialFasta = referenceIsPartial)
}

trait ReferenceArgs extends HasReference {
  @Args4jOption(
    name = "--reference",
    required = true,
    usage = "Local path to a reference FASTA file"
  )
  var referencePath: String = null

  @Args4jOption(
    name = "--partial-reference",
    usage = "Treat the reference fasta as a \"partial FASTA\", comprised of segments (possibly in the interior) of contigs."
  )
  var referenceIsPartial: Boolean = false
}
