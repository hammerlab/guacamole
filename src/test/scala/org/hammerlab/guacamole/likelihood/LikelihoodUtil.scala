package org.hammerlab.guacamole.likelihood

import org.apache.spark.SparkContext
import org.bdgenomics.adam.util.PhredUtils
import org.hammerlab.guacamole.reads.ReadsUtil
import org.hammerlab.guacamole.reference.ReferenceUtil
import org.hammerlab.guacamole.util.Bases
import org.hammerlab.guacamole.variants.{Allele, Genotype}

/**
 * Some utility functions for [[LikelihoodSuite]].
 */
object LikelihoodUtil
  extends ReadsUtil
    with ReferenceUtil {

  def referenceBroadcast(sc: SparkContext) = makeReference(sc, Seq(("chr1", 1, "C")))
  val referenceBase = 'C'.toByte

  def makeGenotype(alleles: String*): Genotype = {
    // If we later change Genotype to work with Array[byte] instead of strings, we can use this function to convert
    // to byte arrays.
    Genotype(alleles.map(allele => Allele(Seq(referenceBase), Bases.stringToBases(allele))): _*)
  }

  def makeGenotype(alleles: (Char, Char)): Genotype = {
    makeGenotype(alleles.productIterator.map(_.toString).toList: _*)
  }

  val errorPhred30 = PhredUtils.phredToErrorProbability(30)
  val errorPhred40 = PhredUtils.phredToErrorProbability(40)

  def refRead(phred: Int) = makeRead("C", "1M", 1, "chr1", Array(phred))
  def altRead(phred: Int) = makeRead("A", "1M", 1, "chr1", Array(phred))
}
