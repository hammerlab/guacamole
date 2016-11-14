package org.hammerlab.guacamole.likelihood

import org.bdgenomics.adam.util.PhredUtils.phredToErrorProbability
import org.hammerlab.guacamole.likelihood.Likelihood.{uniformPrior, likelihoodsOfGenotypes, probabilitiesOfAllPossibleGenotypesFromPileup}
import org.hammerlab.guacamole.pileup.{PileupElement, Util => PileupUtil}
import org.hammerlab.guacamole.reads.{MappedRead, ReadsUtil}
import org.hammerlab.guacamole.reference.ReferenceUtil
import org.hammerlab.guacamole.util.Bases.stringToBases
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.guacamole.variants.{Allele, Genotype}
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.math.{exp, log}

class LikelihoodSuite
  extends GuacFunSuite
    with TableDrivenPropertyChecks
    with ReadsUtil
    with PileupUtil
    with ReferenceUtil {

  val epsilon = 1e-12

  override lazy val reference = makeReference(sc, "chr1", 1, "C")

  val referenceBase = 'C'.toByte

  def makeGenotype(alleles: String*): Genotype = {
    val alleleFraction = 1.0 / alleles.length
    Genotype(
      (for {
        alleleStr <- alleles
        allele = Allele(Seq(referenceBase), stringToBases(alleleStr))
      } yield
        allele -> alleleFraction
      ).toMap
    )
  }

  def makeGenotype(alleles: (Char, Char)): Genotype =
    makeGenotype(
      alleles
        .productIterator
        .map(_.toString)
        .toList
        .distinct
        : _*
    )

  /**
   * Calculate the likelihood of a single genotype.
   */
  def likelihoodOfGenotype(elements: Seq[PileupElement],
                           genotype: Genotype,
                           includeAlignment: Boolean = false,
                           logSpace: Boolean = false): Double = {

    val result =
      likelihoodsOfGenotypes(
        elements,
        Array(genotype),
        includeAlignment,
        uniformPrior,
        logSpace,
        normalize = false
      )

    assert(result.size == 1)
    result(0)
  }

  val errorPhred30 = phredToErrorProbability(30)
  val errorPhred40 = phredToErrorProbability(40)

  val successPhred30 = 1 - errorPhred30
  val successPhred40 = 1 - errorPhred40

  val half = 1.0 / 2

  def refRead(phred: Int) = makeRead("C", "1M", 1, "chr1", Array(phred))
  def altRead(phred: Int) = makeRead("A", "1M", 1, "chr1", Array(phred))

  /**
   * Verify likelihoods of the genotypes found among the provided reads.
   */
  def testLikelihoods(reads: Seq[MappedRead],
                      logSpace: Boolean,
                      expectedLikelihoods: ((Char, Char), Double)*): Unit = {

    val pileup = makePileup(reads, "chr1", 1)

    val actualProbabilities = probabilitiesOfAllPossibleGenotypesFromPileup(pileup, logSpace = logSpace)

    var totalExpectedLikelihood = 0.0
    val expectedGenotypeLikelihoods =
      (for {
        (alleles, likelihood) <- expectedLikelihoods.toList
      } yield {
        totalExpectedLikelihood += (if (logSpace) exp(likelihood) else likelihood)
        makeGenotype(alleles) -> likelihood
      }).toMap

    val expectedProbabilities =
      expectedGenotypeLikelihoods.mapValues(
        likelihood =>
          if (logSpace)
            likelihood - log(totalExpectedLikelihood)
          else
            likelihood / totalExpectedLikelihood
      )

    actualProbabilities.size should equal(expectedProbabilities.size)

    val actualProbabilitiesMap = actualProbabilities.toMap

    forAll(
      Table(
        "genotype",
        expectedProbabilities.toList: _*
      )
    ) {
      case (genotype, expectedProbability) =>
        actualProbabilitiesMap(genotype) should === (expectedProbability +- epsilon)
    }
  }

  def testLikelihoods(reads: Seq[MappedRead],
                      expectedLikelihoods: ((Char, Char), Double)*): Unit =
    testLikelihoods(reads, logSpace = false, expectedLikelihoods: _*)

  /**
   * Verify some given genotypes' likelihoods.
   */
  def testGenotypeLikelihoods(reads: Seq[MappedRead], genotypesMap: ((Char, Char), Double)*): Unit = {

    val pileup = makePileup(reads, reads(0).contigName, 1)

    forAll(
      Table(
        "genotype",
        genotypesMap: _*
      )
    ) {
      case (alleles, expectedLikelihood) =>
        val actualLikelihood =
          likelihoodOfGenotype(
            pileup.elements,
            makeGenotype(alleles)
          )

        actualLikelihood should ===(expectedLikelihood +- epsilon)
    }
  }

  test("all reads ref") {
    testGenotypeLikelihoods(
      Seq(refRead(30), refRead(40), refRead(30)),
      ('C', 'C') -> successPhred30 * successPhred40 * successPhred30,
      ('C', 'A') -> half * half * half,
      ('A', 'C') -> half * half * half,
      ('A', 'A') -> errorPhred30 * errorPhred40 * errorPhred30,
      ('A', 'T') -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("two ref, one alt") {
    testGenotypeLikelihoods(
      Seq(refRead(30), refRead(40), altRead(30)),
      ('C', 'C') -> successPhred30 * successPhred40 * errorPhred30,
      ('C', 'A') -> half * half * half,
      ('A', 'C') -> half * half * half,
      ('A', 'A') -> errorPhred30 * errorPhred40 * successPhred30,
      ('A', 'T') -> errorPhred30 * errorPhred40 * half,
      ('T', 'T') -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("one ref, two alt") {
    testGenotypeLikelihoods(
      Seq(refRead(30), altRead(40), altRead(30)),
      ('C', 'C') -> successPhred30 * errorPhred40 * errorPhred30,
      ('C', 'A') -> half * half * half,
      ('A', 'C') -> half * half * half,
      ('A', 'A') -> errorPhred30 * successPhred40 * successPhred30,
      ('A', 'T') -> errorPhred30 * half * half,
      ('T', 'T') -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("all reads alt") {
    testGenotypeLikelihoods(
      Seq(altRead(30), altRead(40), altRead(30)),
      ('C', 'C') -> errorPhred30 * errorPhred40 * errorPhred30,
      ('C', 'A') -> half * half * half,
      ('A', 'C') -> half * half * half,
      ('A', 'A') -> successPhred30 * successPhred40 * successPhred30,
      ('A', 'T') -> half * half * half,
      ('T', 'T') -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("score genotype for single sample; all bases ref") {
    testLikelihoods(
      Seq(
        refRead(30),
        refRead(40),
        refRead(30)
      ),
      ('C', 'C') -> successPhred30 * successPhred40 * successPhred30
    )
  }

  test("score genotype for single sample; mix of ref/non-ref bases") {
    testLikelihoods(
      Seq(
        refRead(30),
        refRead(40),
        altRead(30)
      ),
      ('C', 'C') -> successPhred30 * successPhred40 * errorPhred30,
      ('A', 'C') -> 1 / 8.0,
      ('A', 'A') -> errorPhred30 * errorPhred40 * successPhred30
    )
  }

  test("score genotype for single sample; all bases non-ref") {
    testLikelihoods(
      Seq(
        altRead(30),
        altRead(40),
        altRead(30)
      ),
      ('A', 'A') -> successPhred30 * successPhred40 * successPhred30
    )
  }

  test("log score genotype for single sample; all bases ref") {
    testLikelihoods(
      Seq(
        refRead(30),
        refRead(40),
        refRead(30)
      ),
      logSpace = true,
      ('C', 'C') -> (log(successPhred30) + log(successPhred40) + log(successPhred30))
    )
  }

  test("log score genotype for single sample; mix of ref/non-ref bases") {
    testLikelihoods(
      Seq(
        refRead(30),
        refRead(40),
        altRead(30)
      ),
      logSpace = true,
      ('C', 'C') -> (log(successPhred30) + log(successPhred40) + log(errorPhred30)),
      ('A', 'C') -> (3 * log(half)),
      ('A', 'A') -> (log(errorPhred30) + log(errorPhred40) + log(successPhred30))
    )
  }

  test("log score genotype for single sample; all bases non-ref") {
    testLikelihoods(
      Seq(
        altRead(30),
        altRead(40),
        altRead(30)
      ),
      logSpace = true,
      ('A', 'A') -> (log(successPhred30) + log(successPhred40) + log(successPhred30))
    )
  }
}
