package org.hammerlab.guacamole.likelihood

import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.variants.Genotype

object PValue {

  def pValue(genotype: Genotype, pileup: Pileup): Double = {
    // weight reads by quality, to derive an adjusted "num matching" and "num nonmatching"
    // compute p value of observing the read counts drawn from a population of the given
    // genotype plus an error population. (e.g. fischer's exact test).

    //genotype.alleles
    2.0
  }
}
