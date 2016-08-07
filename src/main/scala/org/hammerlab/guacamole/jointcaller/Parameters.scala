package org.hammerlab.guacamole.jointcaller

import org.hammerlab.guacamole.logging.DebugLogArgs
import org.kohsuke.args4j.{Option => Args4jOption}

/**
 * Model parameters for the joint caller. Anything that affects the math for the joint caller should go here.
 *
 * See CommandlineArguments for descriptions of these parameters.
 */
case class Parameters(
    maxAllelesPerSite: Int,
    maxCallsPerSite: Int,
    anyAlleleMinSupportingReads: Int,
    anyAlleleMinSupportingPercent: Double,
    germlineNegativeLog10HeterozygousPrior: Double,
    germlineNegativeLog10HomozygousAlternatePrior: Double,
    somaticNegativeLog10VariantPrior: Double,
    somaticNegativeLog10VariantPriorRna: Double,
    somaticNegativeLog10VariantPriorWithRnaEvidence: Double,
    somaticVafFloor: Double,
    somaticMaxGermlineErrorRatePercent: Double,
    somaticGenotypePolicy: Parameters.SomaticGenotypePolicy.Value,
    filterStrandBiasPhred: Double,
    filterSomaticNormalNonreferencePercent: Double,
    filterSomaticNormalDepth: Int) {

  /**
   * Return the parameters as a sequence of (name, value) pairs. This is used to include the parameter metadata in the VCF.
   */
  def asStringPairs(): Seq[(String, String)] = {
    // We use reflection to avoid re-specifying all the parameters.
    // See: http://stackoverflow.com/questions/7457972/getting-public-fields-and-their-respective-values-of-an-instance-in-scala-java
    val fields = this.getClass.getDeclaredFields
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
  trait CommandlineArguments extends DebugLogArgs {
    @Args4jOption(name = "--max-alleles-per-site",
      usage = "maximum number of alt alleles to consider at a site")
    var maxAllelesPerSite: Int = 4

    @Args4jOption(name = "--max-calls-per-site",
      usage = "maximum number of calls to make at a site. 0 indicates unlimited")
    var maxCallsPerSite: Int = 1

    @Args4jOption(name = "--any-allele-min-supporting-reads",
      usage = "min reads to call any allele (somatic or germline)")
    var anyAlleleMinSupportingReads: Int = 2

    @Args4jOption(name = "--any-allele-min-supporting-percent",
      usage = "min percent of reads to call any allele (somatic or germline)")
    var anyAlleleMinSupportingPercent: Double = 2.0

    @Args4jOption(name = "--germline-negative-log10-heterozygous-prior",
      usage = "prior on a germline het call, higher number means fewer calls")
    var germlineNegativeLog10HeterozygousPrior: Double = 4

    @Args4jOption(name = "--germline-negative-log10-homozygous-alternate-prior",
      usage = "prior on a germline hom-alt call")
    var germlineNegativeLog10HomozygousAlternatePrior: Double = 5

    @Args4jOption(name = "--somatic-negative-log10-variant-prior",
      usage = "prior on somatic call, higher number means fewer calls")
    var somaticNegativeLog10VariantPrior: Double = 6

    @Args4jOption(name = "--somatic-negative-log10-variant-prior-rna", usage = "")
    var somaticNegativeLog10VariantPriorRna: Double = 1

    @Args4jOption(name = "--somatic-negative-log10-variant-prior-with-rna-evidence",
      usage = "")
    var somaticNegativeLog10VariantPriorWithRnaEvidence: Double = 1

    @Args4jOption(name = "--somatic-vaf-floor",
      usage = "min VAF to use in the likelihood calculation")
    var somaticVafFloor: Double = 0.05

    @Args4jOption(name = "--somatic-max-germline-error-rate-percent",
      usage = "max germline error rate percent to have a somatic call")
    var somaticMaxGermlineErrorRatePercent: Double = 4.0

    @Args4jOption(name = "--somatic-genotype-policy",
      usage = "how to genotype (e.g. 0/0 vs. 0/1) somatic calls. Valid options: 'presence' (default), 'trigger'. " +
        "'presence' will genotype as a het (0/1) if there is any evidence for that call in the sample " +
        "(num variant reads > 0 and num variant reads > any other non-reference allele). " +
        "'trigger' will genotype a call as a het only if that sample actually triggered the call.")
    var somaticGenotypePolicy: String = "presence"

    @Args4jOption(name = "--filter-strand-bias-phred",
      usage = "filter calls with strand bias p-value exceeding this phred-scaled value")
    var filterStrandBiasPhred: Double = 60

    @Args4jOption(name = "--filter-somatic-normal-nonreference-percent",
      usage = "filter somatic calls with pooled normal dna percent of reads not matching reference lower than this value")
    var filterSomaticNormalNonreferencePercent: Double = 3.0

    @Args4jOption(name = "--filter-somatic-normal-depth",
      usage = "filter somatic calls with total pooled normal dna depth lower than this value")
    var filterSomaticNormalDepth: Int = 30
  }

  def apply(args: CommandlineArguments): Parameters = {
    new Parameters(
      maxAllelesPerSite = args.maxAllelesPerSite,
      maxCallsPerSite = args.maxCallsPerSite,
      anyAlleleMinSupportingReads = args.anyAlleleMinSupportingReads,
      anyAlleleMinSupportingPercent = args.anyAlleleMinSupportingPercent,
      germlineNegativeLog10HeterozygousPrior = args.germlineNegativeLog10HeterozygousPrior,
      germlineNegativeLog10HomozygousAlternatePrior = args.germlineNegativeLog10HomozygousAlternatePrior,
      somaticNegativeLog10VariantPrior = args.somaticNegativeLog10VariantPrior,
      somaticNegativeLog10VariantPriorRna = args.somaticNegativeLog10VariantPriorRna,
      somaticNegativeLog10VariantPriorWithRnaEvidence = args.somaticNegativeLog10VariantPriorWithRnaEvidence,
      somaticVafFloor = args.somaticVafFloor,
      somaticMaxGermlineErrorRatePercent = args.somaticMaxGermlineErrorRatePercent,
      somaticGenotypePolicy = SomaticGenotypePolicy.withName(args.somaticGenotypePolicy),
      filterStrandBiasPhred = args.filterStrandBiasPhred,
      filterSomaticNormalNonreferencePercent = args.filterSomaticNormalNonreferencePercent,
      filterSomaticNormalDepth = args.filterSomaticNormalDepth
    )
  }

  val defaults = Parameters(new CommandlineArguments {})
}
