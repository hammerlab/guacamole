package org.hammerlab.guacamole.readsets.args

import org.hammerlab.genomics.readsets.args.base.PrefixedPathsBase
import org.hammerlab.genomics.readsets.args.impl.{ NoSequenceDictionaryArgs, PathPrefixArg }
import org.hammerlab.genomics.readsets.args.path.{ UnprefixedPath, UnprefixedPathHandler }
import org.hammerlab.genomics.readsets.io.ReadFilterArgs
import org.kohsuke.args4j.{ Option ⇒ Args4jOption }

/** Arguments for accepting two sets of reads (tumor + normal). */
trait TumorNormalReadsArgs
  extends PrefixedPathsBase
    with PathPrefixArg
    with ReadFilterArgs
    with NoSequenceDictionaryArgs {

  @Args4jOption(
    name = "--normal-reads",
    aliases = Array("-n"),
    metaVar = "X",
    required = true,
    handler = classOf[UnprefixedPathHandler],
    usage = "Path to 'normal' aligned reads"
  )
  var normalReads: UnprefixedPath = _

  @Args4jOption(
    name = "--tumor-reads",
    aliases = Array("-t"),
    metaVar = "X",
    required = true,
    handler = classOf[UnprefixedPathHandler],
    usage = "Path to 'tumor' aligned reads"
  )
  var tumorReads: UnprefixedPath = _

  override protected def unprefixedPaths: Array[UnprefixedPath] =
    Array(
      normalReads,
      tumorReads
    )

  val normalSampleName = "normal"
  val tumorSampleName = "tumor"

  override def sampleNames = Array(normalSampleName, tumorSampleName)
}
