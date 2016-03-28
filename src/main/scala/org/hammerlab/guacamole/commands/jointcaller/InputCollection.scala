package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.commands.jointcaller.Input.{ Analyte, TissueType }
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

import scala.collection.mutable

/**
 * Convenience container for zero or more Input instances.
 */
case class InputCollection(items: Seq[Input]) {
  val normalDNA = items.filter(_.normalDNA)
  val normalRNA = items.filter(_.normalRNA)
  val tumorDNA = items.filter(_.tumorDNA)
  val tumorRNA = items.filter(_.tumorRNA)
}

object InputCollection {
  trait Arguments {
    @Argument(required = true, multiValued = true,
      usage = "FILE1 FILE2 FILE3")
    var paths: Array[String] = Array.empty

    @Args4jOption(name = "--tissue-types", handler = classOf[StringArrayOptionHandler],
      usage = "[normal|tumor] ... [normal|tumor]")
    var tissueTypes: Array[String] = Array.empty

    @Args4jOption(name = "--analytes", handler = classOf[StringArrayOptionHandler],
      usage = "[dna|rna] ... [dna|rna]")
    var analytes: Array[String] = Array.empty

    @Args4jOption(name = "--sample-names", handler = classOf[StringArrayOptionHandler],
      usage = "name1 ... nameN")
    var sampleNames: Array[String] = Array.empty
  }

  /**
   * Create an InputCollection from parsed commandline arguments.
   */
  def apply(args: Arguments): InputCollection = {
    apply(paths = args.paths, sampleNames = args.sampleNames, tissueTypes = args.tissueTypes, analytes = args.analytes)
  }

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
  def apply(paths: Seq[String],
            sampleNames: Seq[String] = Seq.empty,
            tissueTypes: Seq[String] = Seq.empty,
            analytes: Seq[String] = Seq.empty): InputCollection = {

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
      case Seq() => Seq("normal").padTo(paths.length, "tumor")
      case other => other
    }
    checkLength("tissue types", defaultedTissueTypes)

    val defaultedAnalytes: Seq[String] = analytes match {
      case Seq() => Seq.fill(paths.length)("dna")
      case other => other
    }
    checkLength("analytes", defaultedAnalytes)

    val defaultedSampleNames: Seq[String] = sampleNames match {
      case Seq() => paths.map(filepath => filepath.split('/').last.stripSuffix(".bam"))
      case other => other
    }
    checkLength("sample names", defaultedSampleNames)

    val inputs = (0 until paths.length).map(index => {
      Input(
        index = index,
        sampleName = defaultedSampleNames(index),
        path = paths(index),
        tissueType = TissueType.withName(defaultedTissueTypes(index)),
        analyte = Analyte.withName(defaultedAnalytes(index)))
    })
    InputCollection(inputs)
  }
}
