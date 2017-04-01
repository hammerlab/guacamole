package org.hammerlab.guacamole.jointcaller

import org.hammerlab.args4s.StringsOptionHandler
import org.hammerlab.genomics.readsets.PerSample
import org.hammerlab.genomics.readsets.args.base.{ Base, InputArgs }
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
    extends InputArgs {
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
   * Create an [[Inputs]] from commandline arguments.
   */
  def apply(args: Arguments): Inputs =
    args
      .paths
      .indices
      .map {
        id ⇒
          Input(
            id = id,
            name = args.sampleNames(id),
            path = args.paths(id),
            tissueType = TissueType.withName(args.tissueTypes(id)),
            analyte = Analyte.withName(args.analytes(id))
          )
      }
}
