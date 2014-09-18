package org.bdgenomics.guacamole.variants

import org.bdgenomics.guacamole.TestUtil.assertAlmostEqual
import org.bdgenomics.guacamole.pileup.Pileup
import org.bdgenomics.guacamole.reads.MappedRead
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{ Matchers, FunSuite }
import org.bdgenomics.guacamole.ReadsUtil._

class GenotypeAllelesSuite extends FunSuite with Matchers with TableDrivenPropertyChecks {

  def testGenotypeLikelihoods(reads: Seq[MappedRead], genotypesMap: (GenotypeAlleles, Double)*): Unit = {
    val pileup = Pileup(reads, 1)
    forAll(Table("genotype", genotypesMap: _*)) { l =>
      assertAlmostEqual(
        l._1.likelihoodOfReads(pileup.elements, includeAlignmentLikelihood = false),
        l._2
      )
    }
  }

  test("all reads ref") {
    testGenotypeLikelihoods(
      Seq(refRead(30), refRead(40), refRead(30)),
      makeGenotype("C", "C") -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30),
      makeGenotype("C", "A") -> 1.0 / 8,
      makeGenotype("A", "C") -> 1.0 / 8,
      makeGenotype("A", "A") -> errorPhred30 * errorPhred40 * errorPhred30,
      makeGenotype("A", "T") -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("two ref, one alt") {
    testGenotypeLikelihoods(
      Seq(refRead(30), refRead(40), altRead(30)),
      makeGenotype("C", "C") -> (1 - errorPhred30) * (1 - errorPhred40) * errorPhred30,
      makeGenotype("C", "A") -> 1.0 / 8,
      makeGenotype("A", "C") -> 1.0 / 8,
      makeGenotype("A", "A") -> errorPhred30 * errorPhred40 * (1 - errorPhred30),
      makeGenotype("A", "T") -> errorPhred30 * errorPhred40 * 1 / 2,
      makeGenotype("T", "T") -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("one ref, two alt") {
    testGenotypeLikelihoods(
      Seq(refRead(30), altRead(40), altRead(30)),
      makeGenotype("C", "C") -> (1 - errorPhred30) * errorPhred40 * errorPhred30,
      makeGenotype("C", "A") -> 1.0 / 8,
      makeGenotype("A", "C") -> 1.0 / 8,
      makeGenotype("A", "A") -> errorPhred30 * (1 - errorPhred40) * (1 - errorPhred30),
      makeGenotype("A", "T") -> errorPhred30 * 1 / 2 * 1 / 2,
      makeGenotype("T", "T") -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("all reads alt") {
    testGenotypeLikelihoods(
      Seq(altRead(30), altRead(40), altRead(30)),
      makeGenotype("C", "C") -> errorPhred30 * errorPhred40 * errorPhred30,
      makeGenotype("C", "A") -> 1.0 / 8,
      makeGenotype("A", "C") -> 1.0 / 8,
      makeGenotype("A", "A") -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30),
      makeGenotype("A", "T") -> 1.0 / 8,
      makeGenotype("T", "T") -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }
}
