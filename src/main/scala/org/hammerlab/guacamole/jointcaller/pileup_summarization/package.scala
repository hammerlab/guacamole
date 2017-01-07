package org.hammerlab.guacamole.jointcaller

package object pileup_summarization {
  /** Map from sequenced allele -> variant allelic fraction. The allelic fractions should sum to 1. */
  type AlleleMixture = Map[String, Double]

  object AlleleMixture {
    def apply(entries: (String, Double)*): AlleleMixture = entries.toMap
  }
}
