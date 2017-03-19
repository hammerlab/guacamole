package org.hammerlab.guacamole.jointcaller

import org.apache.hadoop.fs.Path
import org.hammerlab.args4s.StringsOptionHandler
import org.hammerlab.genomics.readsets.args.base.Base
import org.hammerlab.genomics.readsets.{ PerSample, SampleName }
import org.hammerlab.guacamole.jointcaller.Sample.{ Analyte, TissueType }
import org.kohsuke.args4j

case class Inputs(inputs: PerSample[Input])
  extends SamplesI[Input] {
  override def samples: PerSample[Input] = inputs
}

object Inputs {

  implicit def make(inputs: PerSample[Input]): Inputs = Inputs(inputs)
  implicit def unmake(inputs: Inputs): PerSample[Input] = inputs.inputs

  trait Arguments
    extends Base {
    @args4j.Option(
      name = "--tissue-types",
      handler = classOf[StringsOptionHandler],
      usage = "[normal|tumor],…,[normal|tumor]"
    )
    var tissueTypes: Array[String] = Array()

    @args4j.Option(
      name = "--analytes",
      handler = classOf[StringsOptionHandler],
      usage = "[dna|rna],…,[dna|rna]"
    )
    var analytes: Array[String] = Array()
  }

  /**
   * Create an InputCollection from parsed commandline arguments.
   */
  def apply(args: Arguments): Inputs =
    apply(
      paths = args.paths.toVector,
      sampleNames = args.sampleNames.toVector,
      tissueTypes = args.tissueTypes.toVector,
      analytes = args.analytes.toVector
    )

  /**
   * Create an InputCollection of one or more inputs.
   *
   * Everything except paths is optional. If not specified, the sample names defaults to the basename of the file path
   * with the suffix ".bam" removed, tissue types default to normal for the first sample and tumor for the rest, and
   * analytes defaults to "dna".
   *
   * @param paths file paths
   * @param sampleNames names to use in VCF output
   * @param tissueTypes "normal" or "tumor" for each input
   * @param analytes "dna" or "rna" for each input
   * @return resulting InputCollection
   */
  def apply(paths: PerSample[Path],
            sampleNames: PerSample[SampleName] = Vector.empty,
            tissueTypes: PerSample[String] = Vector.empty,
            analytes: PerSample[String] = Vector.empty): Inputs = {

    def checkLength(name: String, items: Seq[String]) = {
      if (items.length != paths.length) {
        throw new IllegalArgumentException("%d BAM inputs specified but %d %s specified".format(
          paths.length, items.length, name))
      }
    }

    if (paths.isEmpty) {
      throw new IllegalArgumentException("No input BAMs specified.")
    }

    val defaultedTissueTypes: Seq[String] = tissueTypes match {
      case Seq() ⇒ Seq("normal").padTo(paths.length, "tumor")
      case other ⇒ other
    }
    checkLength("tissue types", defaultedTissueTypes)

    val defaultedAnalytes: Seq[String] = analytes match {
      case Seq() ⇒ Seq.fill(paths.length)("dna")
      case other ⇒ other
    }
    checkLength("analytes", defaultedAnalytes)

    val defaultedSampleNames: PerSample[SampleName] = sampleNames match {
      case Seq() ⇒ paths.map(path ⇒ path.getName.stripSuffix(".bam"))
      case other ⇒ other
    }
    checkLength("sample names", defaultedSampleNames)

    val inputs =
      paths.indices.map {
        id ⇒
          Input(
            id = id,
            name = defaultedSampleNames(id),
            path = paths(id),
            tissueType = TissueType.withName(defaultedTissueTypes(id)),
            analyte = Analyte.withName(defaultedAnalytes(id))
          )
      }

    inputs
  }
}
