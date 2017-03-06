package org.hammerlab.guacamole.jointcaller

import org.hammerlab.genomics.bases.Bases

package object pileup_summarization {
  /** Map from sequenced allele → variant allelic fraction. The allelic fractions should sum to 1. */
  type AlleleMixture = Map[Bases, Double]

  object AlleleMixture {
    def apply(entries: (Bases, Double)*): AlleleMixture = entries.toMap
  }
}
