package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole.{ Bases, TestUtil }
import org.bdgenomics.guacamole.genotype.GenotypeAlleles
import org.bdgenomics.guacamole.pileup.Pileup
import org.bdgenomics.adam.util.PhredUtils

class BayesianQualityVariantCallerSuite extends SparkFunSuite {

  def makeGenotype(alleles: String*): GenotypeAlleles = {
    // If we later change Genotype to work with Array[byte] instead of strings, we can use this function to convert
    // to byte arrays.
    GenotypeAlleles(alleles.map(Bases.stringToBases(_)): _*)
  }

  val floatingPointingThreshold = 1e-6
  val errorPhred30 = PhredUtils.phredToErrorProbability(30)
  val errorPhred40 = PhredUtils.phredToErrorProbability(40)

  test("score genotype for single sample, all bases ref") {

    val reads = Seq(
      TestUtil.makeRead("C", "1M", "1", 1, "chr1", Some(Array(30))),
      TestUtil.makeRead("C", "1M", "1", 1, "chr1", Some(Array(40))),
      TestUtil.makeRead("C", "1M", "1", 1, "chr1", Some(Array(30))))

    val pileup = Pileup(reads, 1)
    val hetLikelihood = ((1 - errorPhred30) + errorPhred30 / 3) * ((1 - errorPhred40) + errorPhred40 / 3) * ((1 - errorPhred30) + errorPhred30 / 3) / 8.0
    val altLikelihood = (2 * errorPhred30 / 3) * (2 * errorPhred40 / 3) * (2 * errorPhred30 / 3) / 8.0
    val expectedLikelihoods = scala.collection.mutable.Map.empty[GenotypeAlleles, Double]

    expectedLikelihoods += makeGenotype("C", "C") -> (2 * ((1 - errorPhred30) * 2 * (1 - errorPhred40) * 2 * (1 - errorPhred30))) / 8.0

    expectedLikelihoods += makeGenotype("A", "C") -> hetLikelihood
    expectedLikelihoods += makeGenotype("C", "G") -> hetLikelihood
    expectedLikelihoods += makeGenotype("T", "C") -> hetLikelihood

    expectedLikelihoods += makeGenotype("A", "A") -> altLikelihood
    expectedLikelihoods += makeGenotype("G", "G") -> altLikelihood
    expectedLikelihoods += makeGenotype("T", "G") -> altLikelihood

    val scored = BayesianQualityVariantCaller.computeLikelihoods(pileup).toMap
    scored.foreach(l => TestUtil.assertAlmostEqual(l._2, expectedLikelihoods(l._1), 1e-2))
  }

  test("score genotype for single sample, mix of ref/non-ref bases") {
    val reads = Seq(
      TestUtil.makeRead("C", "1M", "1", 1, "chr1", Some(Array(30))),
      TestUtil.makeRead("C", "1M", "1", 1, "chr1", Some(Array(40))),
      TestUtil.makeRead("A", "1M", "0A0", 1, "chr1", Some(Array(30))))

    val pileup = Pileup(reads, 1)

    val hetLikelihood = ((1 - errorPhred30) + errorPhred30 / 3) * ((1 - errorPhred40) + errorPhred40 / 3) * ((1 - errorPhred30) + errorPhred30 / 3) / 8.0
    val expectedLikelihoods = scala.collection.mutable.Map.empty[GenotypeAlleles, Double]

    expectedLikelihoods += makeGenotype("C", "C") -> (2 * ((1 - errorPhred30) * 2 * (1 - errorPhred40) * 2 * errorPhred30)) / 8.0
    expectedLikelihoods += makeGenotype("A", "C") -> hetLikelihood
    expectedLikelihoods += makeGenotype("A", "A") -> (2 * errorPhred30 * 2 * errorPhred40 * 2 * (1 - errorPhred30)) / 8.0

    val scored = BayesianQualityVariantCaller.computeLikelihoods(pileup).toMap
    scored.foreach(l => TestUtil.assertAlmostEqual(l._2, expectedLikelihoods(l._1), 1e-3))
  }

  test("score genotype for single sample, all bases non-ref") {

    val reads = Seq(
      TestUtil.makeRead("A", "1M", "0A0", 1, "chr1", Some(Array(30))),
      TestUtil.makeRead("A", "1M", "0A0", 1, "chr1", Some(Array(40))),
      TestUtil.makeRead("A", "1M", "0A0", 1, "chr1", Some(Array(30))))

    val pileup = Pileup(reads, 1)

    val hetLikelihood = ((1 - errorPhred30) + errorPhred30 / 3) * ((1 - errorPhred40) + errorPhred40 / 3) * ((1 - errorPhred30) + errorPhred30 / 3) / 8.0
    val allErrorLikelihood = (2 * errorPhred30 / 3) * (2 * errorPhred40 / 3) * (2 * errorPhred30 / 3) / 8.0
    val expectedLikelihoods = scala.collection.mutable.Map.empty[GenotypeAlleles, Double]

    expectedLikelihoods += makeGenotype("A", "A") -> (2 * ((1 - errorPhred30) * 2 * (1 - errorPhred40) * 2 * (1 - errorPhred30))) / 8.0

    expectedLikelihoods += makeGenotype("A", "C") -> hetLikelihood
    expectedLikelihoods += makeGenotype("A", "G") -> hetLikelihood
    expectedLikelihoods += makeGenotype("A", "T") -> hetLikelihood

    expectedLikelihoods += makeGenotype("T", "T") -> hetLikelihood
    expectedLikelihoods += makeGenotype("G", "G") -> hetLikelihood

    expectedLikelihoods += makeGenotype("C", "C") -> allErrorLikelihood

    val scored = BayesianQualityVariantCaller.computeLikelihoods(pileup).toMap
    scored.foreach(l => TestUtil.assertAlmostEqual(l._2, expectedLikelihoods(l._1), 1e-2))
  }

  test("log score genotype for single sample, all bases ref") {

    val reads = Seq(
      TestUtil.makeRead("C", "1M", "1", 1, "chr1", Some(Array(30))),
      TestUtil.makeRead("C", "1M", "1", 1, "chr1", Some(Array(40))),
      TestUtil.makeRead("C", "1M", "1", 1, "chr1", Some(Array(30))))

    val pileup = Pileup(reads, 1)
    val hetLikelihood = math.log((1 - errorPhred30) + errorPhred30 / 3) + math.log((1 - errorPhred40) + errorPhred40 / 3) + math.log((1 - errorPhred30) + errorPhred30 / 3) - 3.0
    val altLikelihood = math.log(2 * errorPhred30 / 3) + math.log(2 * errorPhred40 / 3) + math.log(2 * errorPhred30 / 3) - 3.0
    val expectedLikelihoods = scala.collection.mutable.Map.empty[GenotypeAlleles, Double]

    expectedLikelihoods += makeGenotype("C", "C") -> (math.log(2 * (1 - errorPhred30)) + math.log(2 * (1 - errorPhred40)) + math.log(2 * (1 - errorPhred30)) - 3)

    expectedLikelihoods += makeGenotype("A", "C") -> hetLikelihood
    expectedLikelihoods += makeGenotype("C", "G") -> hetLikelihood
    expectedLikelihoods += makeGenotype("T", "C") -> hetLikelihood

    expectedLikelihoods += makeGenotype("A", "A") -> altLikelihood
    expectedLikelihoods += makeGenotype("G", "G") -> altLikelihood
    expectedLikelihoods += makeGenotype("T", "G") -> altLikelihood

    val scored = BayesianQualityVariantCaller.computeLogLikelihoods(pileup).toMap
    scored.foreach(l => TestUtil.assertAlmostEqual(l._2, expectedLikelihoods(l._1)))
  }

  test("log score genotype for single sample, mix of ref/non-ref bases") {
    val reads = Seq(
      TestUtil.makeRead("C", "1M", "1", 1, "chr1", Some(Array(30))),
      TestUtil.makeRead("C", "1M", "1", 1, "chr1", Some(Array(40))),
      TestUtil.makeRead("A", "1M", "0A0", 1, "chr1", Some(Array(30))))

    val pileup = Pileup(reads, 1)

    val hetLikelihood = math.log((1 - errorPhred30) + errorPhred30 / 3) + math.log((1 - errorPhred40) + errorPhred40 / 3) + math.log((1 - errorPhred30) + errorPhred30 / 3) - 3.0
    val expectedLikelihoods = scala.collection.mutable.Map.empty[GenotypeAlleles, Double]

    expectedLikelihoods += makeGenotype("C", "C") -> (math.log(2 * (1 - errorPhred30)) + math.log(2 * (1 - errorPhred40)) + math.log(2 * errorPhred30) - 3.0)
    expectedLikelihoods += makeGenotype("A", "C") -> hetLikelihood
    expectedLikelihoods += makeGenotype("A", "A") -> (math.log(2 * errorPhred30) + math.log(2 * errorPhred40) + math.log(2 * (1 - errorPhred30)) - 3.0)

    val scored = BayesianQualityVariantCaller.computeLogLikelihoods(pileup).toMap
    scored.foreach(l => TestUtil.assertAlmostEqual(l._2, expectedLikelihoods(l._1), 1e-2))
  }

  test("log score genotype for single sample, all bases non-ref") {

    val reads = Seq(
      TestUtil.makeRead("A", "1M", "0A0", 1, "chr1", Some(Array(30))),
      TestUtil.makeRead("A", "1M", "0A0", 1, "chr1", Some(Array(40))),
      TestUtil.makeRead("A", "1M", "0A0", 1, "chr1", Some(Array(30))))

    val pileup = Pileup(reads, 1)

    val hetLikelihood = math.log((1 - errorPhred30) + errorPhred30 / 3) + math.log((1 - errorPhred40) + errorPhred40 / 3) + math.log((1 - errorPhred30) + errorPhred30 / 3) - 3.0
    val allErrorLikelihood = math.log(2 * errorPhred30 / 3) + math.log(2 * errorPhred40 / 3) + math.log(2 * errorPhred30 / 3) - 3.0
    val expectedLikelihoods = scala.collection.mutable.Map.empty[GenotypeAlleles, Double]

    expectedLikelihoods += makeGenotype("A", "A") -> (math.log(2 * (1 - errorPhred30)) + math.log(2.0 * (1 - errorPhred30)) + math.log(2 * (1 - errorPhred30)) - 3.0)

    expectedLikelihoods += makeGenotype("A", "C") -> hetLikelihood
    expectedLikelihoods += makeGenotype("A", "G") -> hetLikelihood
    expectedLikelihoods += makeGenotype("A", "T") -> hetLikelihood

    expectedLikelihoods += makeGenotype("T", "T") -> hetLikelihood
    expectedLikelihoods += makeGenotype("G", "G") -> hetLikelihood

    expectedLikelihoods += makeGenotype("C", "C") -> allErrorLikelihood

    val scored = BayesianQualityVariantCaller.computeLogLikelihoods(pileup).toMap
    scored.foreach(l => TestUtil.assertAlmostEqual(l._2, expectedLikelihoods(l._1), 1e-2))
  }

}
