
package org.bdgenomics.guacamole

import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.guacamole.variants.{ Allele, Genotype }

/**
 * Some utility functions shared by [[org.bdgenomics.guacamole.pileup.PileupLikelihoodSuite]] and
 * [[org.bdgenomics.guacamole.variants.GenotypeSuite]].
 */
object ReadsUtil {

  val referenceBase = 'C'.toByte

  def makeGenotype(alleles: String*): Genotype = {
    // If we later change Genotype to work with Array[byte] instead of strings, we can use this function to convert
    // to byte arrays.
    Genotype(alleles.map(allele => Allele(Seq(referenceBase), Bases.stringToBases(allele))): _*)
  }

  val errorPhred30 = PhredUtils.phredToErrorProbability(30)
  val errorPhred40 = PhredUtils.phredToErrorProbability(40)

  def refRead(phred: Int) = TestUtil.makeRead("C", "1M", "1", 1, "chr1", Some(Array(phred)))
  def altRead(phred: Int) = TestUtil.makeRead("A", "1M", "0C0", 1, "chr1", Some(Array(phred)))

}
