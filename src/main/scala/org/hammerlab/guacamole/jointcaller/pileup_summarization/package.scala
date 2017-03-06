package org.hammerlab.guacamole.jointcaller

import org.hammerlab.genomics.bases.Bases
import org.hammerlab.guacamole.jointcaller.pileup_summarization.PileupStats

package object pileup_summarization {
  /** Map from sequenced allele → variant allelic fraction. The allelic fractions should sum to 1. */
  implicit class AlleleMixture(val map: Map[Bases, Double]) extends AnyVal
  object AlleleMixture {
    implicit def unwrapMixture(mixture: AlleleMixture): Map[Bases, Double] = mixture.map
    def apply(entries: (Bases, Double)*): AlleleMixture = AlleleMixture(entries.toMap)
  }
}
