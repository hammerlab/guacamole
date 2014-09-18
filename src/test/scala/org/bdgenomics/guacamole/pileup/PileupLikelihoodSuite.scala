
package org.bdgenomics.guacamole.pileup

import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole.variants.Genotype
import org.bdgenomics.guacamole.TestUtil
import org.bdgenomics.guacamole.ReadsUtil._
import org.scalatest.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class PileupLikelihoodSuite extends SparkFunSuite with TableDrivenPropertyChecks with Matchers {

  def testLikelihoods(actualLikelihoods: Seq[(Genotype, Double)],
                      expectedLikelihoods: Map[Genotype, Double],
                      acceptableError: Double = 1e-12): Unit = {
    actualLikelihoods.size should equal(expectedLikelihoods.size)
    val actualLikelihoodsMap = actualLikelihoods.toMap
    forAll(Table("genotype", expectedLikelihoods.toList: _*)) {
      l => TestUtil.assertAlmostEqual(actualLikelihoodsMap(l._1), l._2, acceptableError)
    }
  }

  test("score genotype for single sample; all bases ref") {

    val reads = Seq(
      refRead(30),
      refRead(40),
      refRead(30)
    )

    val pileup = Pileup(reads, 1)

    val expectedLikelihoods: Map[Genotype, Double] =
      Map(
        makeGenotype("C", "C") -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30)
      )

    testLikelihoods(pileup.computeLikelihoods(includeAlignmentLikelihood = false), expectedLikelihoods)
  }

  test("score genotype for single sample; mix of ref/non-ref bases") {
    val reads = Seq(
      refRead(30),
      refRead(40),
      altRead(30)
    )

    val pileup = Pileup(reads, 1)

    val expectedLikelihoods: Map[Genotype, Double] =
      Map(
        makeGenotype("C", "C") -> (1 - errorPhred30) * (1 - errorPhred40) * errorPhred30,
        makeGenotype("A", "C") -> 1 / 8.0,
        makeGenotype("A", "A") -> errorPhred30 * errorPhred40 * (1 - errorPhred30)
      )

    testLikelihoods(pileup.computeLikelihoods(includeAlignmentLikelihood = false), expectedLikelihoods)
  }

  test("score genotype for single sample; all bases non-ref") {

    val reads = Seq(
      altRead(30),
      altRead(40),
      altRead(30)
    )

    val pileup = Pileup(reads, 1)

    val expectedLikelihoods: Map[Genotype, Double] =
      Map(
        makeGenotype("A", "A") -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30)
      )

    testLikelihoods(pileup.computeLikelihoods(includeAlignmentLikelihood = false), expectedLikelihoods)
  }

  test("log score genotype for single sample; all bases ref") {

    val reads = Seq(
      refRead(30),
      refRead(40),
      refRead(30)
    )

    val pileup = Pileup(reads, 1)

    val expectedLikelihoods: Map[Genotype, Double] =
      Map(
        makeGenotype("C", "C") ->
          (math.log(1 - errorPhred30) + math.log(1 - errorPhred40) + math.log(1 - errorPhred30))
      )

    testLikelihoods(pileup.computeLogLikelihoods(), expectedLikelihoods)
  }

  test("log score genotype for single sample; mix of ref/non-ref bases") {
    val reads = Seq(
      refRead(30),
      refRead(40),
      altRead(30)
    )

    val pileup = Pileup(reads, 1)

    val expectedLikelihoods: Map[Genotype, Double] =
      Map(

        makeGenotype("C", "C") ->
          (math.log(1 - errorPhred30) + math.log(1 - errorPhred40) + math.log(errorPhred30)),

        makeGenotype("A", "C") -> math.log(1.0 / 8),

        makeGenotype("A", "A") ->
          (math.log(errorPhred30) + math.log(errorPhred40) + math.log(1 - errorPhred30))
      )

    testLikelihoods(pileup.computeLogLikelihoods(), expectedLikelihoods)
  }

  test("log score genotype for single sample; all bases non-ref") {

    val reads = Seq(
      altRead(30),
      altRead(40),
      altRead(30)
    )

    val pileup = Pileup(reads, 1)

    val expectedLikelihoods: Map[Genotype, Double] =
      Map(
        makeGenotype("A", "A") ->
          (math.log(1 - errorPhred30) + math.log(1 - errorPhred40) + math.log(1 - errorPhred30))
      )

    testLikelihoods(pileup.computeLogLikelihoods(), expectedLikelihoods)
  }

}
