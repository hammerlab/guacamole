package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.Common
import org.kohsuke.args4j.{Option => Args4jOption}

/**
 * Model parameters for the joint caller. Anything that affects the math for the joint caller should go here.
 *
 * See CommandlineArguments for descriptions of these parameters.
 */
case class Parameters(
    anyAlleleMinSupportingReads: Int,
    anyAlleleMinSupportingPercent: Double,
    germlineNegativeLog10HeterozygousPrior: Double,
    germlineNegativeLog10HomozygousAlternatePrior: Double,
    somaticNegativeLog10VariantPrior: Double,
    somaticVafFloor: Double,
    somaticMaxGermlineErrorRatePercent: Double,
    somaticGenotypePolicy: Parameters.SomaticGenotypePolicy.Value) {

  /**
   * Return the parameters as a sequence of (name, value) pairs. This is used to include the parameter metadata in the VCF.
   */
  def asStringPairs(): Seq[(String, String)] = {
    // We use reflection to avoid re-specifying all the parameters.
    // See: http://stackoverflow.com/questions/7457972/getting-public-fields-and-their-respective-values-of-an-instance-in-scala-java
    val fields = this.getClass.getDeclaredFields()
    for (field <- fields) yield {
      field.setAccessible(true)
      (field.getName, field.get(this).toString)
    }
  }
}
object Parameters {
  object SomaticGenotypePolicy extends Enumeration {
    val Presence = Value("presence")
    val Trigger = Value("trigger")
  }

  /** Commandline arguments to specify each parameter. */
  trait CommandlineArguments extends Common.Arguments.Base {
    @Args4jOption(name = "--any-allele-min-supporting-reads", usage = "min reads to call any allele (somatic or germline)")
    var anyAlleleMinSupportingReads: Int = 2

    @Args4jOption(name = "--any-allele-min-supporting-percent", usage = "min percent of reads to call any allele (somatic or germline)")
    var anyAlleleMinSupportingPercent: Double = 3.0

    @Args4jOption(name = "--germline-negative-log10-heterozygous-prior", usage = "prior on a germline het call, higher number means fewer calls")
    var germlineNegativeLog10HeterozygousPrior: Double = 4

    @Args4jOption(name = "--germline-negative-log10-homozygous-alternate-prior", usage = "prior on a germline hom-alt call")
    var germlineNegativeLog10HomozygousAlternatePrior: Double = 5

    @Args4jOption(name = "--somatic-negative-log10-variant-prior", usage = "prior on somatic call, higher number means fewer calls")
    var somaticNegativeLog10VariantPrior: Double = 6

    @Args4jOption(name = "--somatic-vaf-floor", usage = "min VAF to use in the likelihood calculation")
    var somaticVafFloor: Double = 0.05

    @Args4jOption(name = "--somatic-max-germline-error-rate-percent", usage = "max germline error rate percent to have a somatic call")
    var somaticMaxGermlineErrorRatePercent: Double = 4.0

    @Args4jOption(name = "--somatic-genotype-policy",
      usage = "how to genotype (e.g. 0/0 vs. 0/1) somatic calls. Valid options: 'presence' (default), 'trigger'. " +
        "'presence' will genotype as a het (0/1) if there is any evidence for that call in the sample " +
        "(num variant reads > 0 and num variant reads > any other non-reference allele). " +
        "'trigger' will genotype a call as a het only if that sample actually triggered the call.")
    var somaticGenotypePolicy: String = "presence"
  }

  def apply(args: CommandlineArguments): Parameters = {
    new Parameters(
      anyAlleleMinSupportingReads = args.anyAlleleMinSupportingReads,
      anyAlleleMinSupportingPercent = args.anyAlleleMinSupportingPercent,
      germlineNegativeLog10HeterozygousPrior = args.germlineNegativeLog10HeterozygousPrior,
      germlineNegativeLog10HomozygousAlternatePrior = args.germlineNegativeLog10HomozygousAlternatePrior,
      somaticNegativeLog10VariantPrior = args.somaticNegativeLog10VariantPrior,
      somaticVafFloor = args.somaticVafFloor,
      somaticMaxGermlineErrorRatePercent = args.somaticMaxGermlineErrorRatePercent,
      somaticGenotypePolicy = SomaticGenotypePolicy.withName(args.somaticGenotypePolicy))
  }

  val defaults = Parameters(new CommandlineArguments {})

}