package org.hammerlab.guacamole.filters.somatic

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.variants.CalledSomaticAllele

object SomaticVAFFilter {

  def hasMinimumVAF(somaticGenotype: CalledSomaticAllele,
                    minVAF: Int): Boolean = {

    somaticGenotype.tumorVariantEvidence.variantAlleleFrequency * 100.0 > minVAF
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minVAF minimum variant allele frequency
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with variant allele frequency >= minVAF
   */
  def apply(genotypes: RDD[CalledSomaticAllele],
            minVAF: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumVAF(_, minVAF))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}
