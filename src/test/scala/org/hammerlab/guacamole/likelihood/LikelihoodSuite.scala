package org.hammerlab.guacamole.likelihood

import org.bdgenomics.adam.util.PhredUtils
import org.hammerlab.guacamole.likelihood.Likelihood.{likelihoodOfGenotype, likelihoodsOfAllPossibleGenotypesFromPileup}
import org.hammerlab.guacamole.pileup.{Util => PileupUtil}
import org.hammerlab.guacamole.reads.{MappedRead, ReadsUtil}
import org.hammerlab.guacamole.reference.ReferenceUtil
import org.hammerlab.guacamole.util.Bases.stringToBases
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.guacamole.variants.{Allele, Genotype}
import org.scalatest.prop.TableDrivenPropertyChecks

class LikelihoodSuite
  extends GuacFunSuite
    with TableDrivenPropertyChecks
    with ReadsUtil
    with PileupUtil
    with ReferenceUtil {

  val epsilon = 1e-12

  override lazy val reference = makeReference(sc, Seq(("chr1", 1, "C")))

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

  val errorPhred30 = PhredUtils.phredToErrorProbability(30)
  val errorPhred40 = PhredUtils.phredToErrorProbability(40)

  def refRead(phred: Int) = makeRead("C", "1M", 1, "chr1", Array(phred))
  def altRead(phred: Int) = makeRead("A", "1M", 1, "chr1", Array(phred))

  def testLikelihoods(actualLikelihoods: Seq[(Genotype, Double)],
                      expectedLikelihoods: ((Char, Char), Double)*): Unit =
    testLikelihoods(
      actualLikelihoods,
      (for {
        (alleles, probability) <- expectedLikelihoods.toList
      } yield
        makeGenotype(alleles) -> probability
      ).toMap
    )

  def testLikelihoods(actualLikelihoods: Seq[(Genotype, Double)],
                      expectedLikelihoods: Map[Genotype, Double]): Unit = {

    actualLikelihoods.size should equal(expectedLikelihoods.size)

    val actualLikelihoodsMap = actualLikelihoods.toMap

    forAll(
      Table(
        "genotype",
        expectedLikelihoods.toList: _*
      )
    ) {
      case (genotype, likelihood) =>
        actualLikelihoodsMap(genotype) should === (likelihood +- epsilon)
    }
  }

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
      ('C', 'C') -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30),
      ('C', 'A') -> 1.0 / 8,
      ('A', 'C') -> 1.0 / 8,
      ('A', 'A') -> errorPhred30 * errorPhred40 * errorPhred30,
      ('A', 'T') -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("two ref, one alt") {
    testGenotypeLikelihoods(
      Seq(refRead(30), refRead(40), altRead(30)),
      ('C', 'C') -> (1 - errorPhred30) * (1 - errorPhred40) * errorPhred30,
      ('C', 'A') -> 1.0 / 8,
      ('A', 'C') -> 1.0 / 8,
      ('A', 'A') -> errorPhred30 * errorPhred40 * (1 - errorPhred30),
      ('A', 'T') -> errorPhred30 * errorPhred40 * 1 / 2,
      ('T', 'T') -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("one ref, two alt") {
    testGenotypeLikelihoods(
      Seq(refRead(30), altRead(40), altRead(30)),
      ('C', 'C') -> (1 - errorPhred30) * errorPhred40 * errorPhred30,
      ('C', 'A') -> 1.0 / 8,
      ('A', 'C') -> 1.0 / 8,
      ('A', 'A') -> errorPhred30 * (1 - errorPhred40) * (1 - errorPhred30),
      ('A', 'T') -> errorPhred30 * 1 / 2 * 1 / 2,
      ('T', 'T') -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("all reads alt") {
    testGenotypeLikelihoods(
      Seq(altRead(30), altRead(40), altRead(30)),
      ('C', 'C') -> errorPhred30 * errorPhred40 * errorPhred30,
      ('C', 'A') -> 1.0 / 8,
      ('A', 'C') -> 1.0 / 8,
      ('A', 'A') -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30),
      ('A', 'T') -> 1.0 / 8,
      ('T', 'T') -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("score genotype for single sample; all bases ref") {
    val reads = Seq(
      refRead(30),
      refRead(40),
      refRead(30)
    )

    val pileup = makePileup(reads, "chr1", 1)

    testLikelihoods(
      likelihoodsOfAllPossibleGenotypesFromPileup(pileup),
      ('C', 'C') -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30)
    )
  }

  test("score genotype for single sample; mix of ref/non-ref bases") {
    val reads = Seq(
      refRead(30),
      refRead(40),
      altRead(30)
    )

    val pileup = makePileup(reads, "chr1", 1)

    testLikelihoods(
      likelihoodsOfAllPossibleGenotypesFromPileup(pileup),
      ('C', 'C') -> (1 - errorPhred30) * (1 - errorPhred40) * errorPhred30,
      ('A', 'C') -> 1 / 8.0,
      ('A', 'A') -> errorPhred30 * errorPhred40 * (1 - errorPhred30)
    )
  }

  test("score genotype for single sample; all bases non-ref") {
    val reads = Seq(
      altRead(30),
      altRead(40),
      altRead(30)
    )

    val pileup = makePileup(reads, "chr1", 1)

    testLikelihoods(
      likelihoodsOfAllPossibleGenotypesFromPileup(pileup),
      ('A', 'A') -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30)
    )
  }

  test("log score genotype for single sample; all bases ref") {
    val reads = Seq(
      refRead(30),
      refRead(40),
      refRead(30)
    )

    val pileup = makePileup(reads, "chr1", 1)

    testLikelihoods(
      likelihoodsOfAllPossibleGenotypesFromPileup(
        pileup,
        logSpace = true
      ),
      ('C', 'C') -> (math.log(1 - errorPhred30) + math.log(1 - errorPhred40) + math.log(1 - errorPhred30))
    )
  }

  test("log score genotype for single sample; mix of ref/non-ref bases") {
    val reads = Seq(
      refRead(30),
      refRead(40),
      altRead(30)
    )

    val pileup = makePileup(reads, "chr1", 1)

    testLikelihoods(
      likelihoodsOfAllPossibleGenotypesFromPileup(
        pileup,
        logSpace = true
      ),
      ('C', 'C') -> (math.log(1 - errorPhred30) + math.log(1 - errorPhred40) + math.log(errorPhred30)),
      ('A', 'C') -> math.log(1.0 / 8),
      ('A', 'A') -> (math.log(errorPhred30) + math.log(errorPhred40) + math.log(1 - errorPhred30))
    )
  }

  test("log score genotype for single sample; all bases non-ref") {
    val reads = Seq(
      altRead(30),
      altRead(40),
      altRead(30)
    )

    val pileup = makePileup(reads, "chr1", 1)

    testLikelihoods(
      likelihoodsOfAllPossibleGenotypesFromPileup(
        pileup,
        logSpace = true
      ),
      ('A', 'A') -> (math.log(1 - errorPhred30) + math.log(1 - errorPhred40) + math.log(1 - errorPhred30))
    )
  }
}
