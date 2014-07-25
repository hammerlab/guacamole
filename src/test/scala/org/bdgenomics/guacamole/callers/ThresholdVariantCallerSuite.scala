package org.bdgenomics.guacamole.callers

import org.scalatest.FunSuite
import org.bdgenomics.guacamole.{ TestUtil }
import org.bdgenomics.formats.avro.ADAMGenotypeAllele
import scala.collection.JavaConversions._
import org.bdgenomics.guacamole.pileup.Pileup
import org.scalatest.matchers.ShouldMatchers

class ThresholdVariantCallerSuite extends FunSuite with ShouldMatchers {

  test("no variants, threshold 0") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = ThresholdVariantCaller.callVariantsAtLocus(pileup, 0)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Ref)))
  }

  test("het variant, threshold 0") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = ThresholdVariantCaller.callVariantsAtLocus(pileup, 0)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)))

  }

  test("het variant, threshold 30") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = ThresholdVariantCaller.callVariantsAtLocus(pileup, 30)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)))

  }

  test("het variant, threshold 50, not enough evidence") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = ThresholdVariantCaller.callVariantsAtLocus(pileup, 50)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Ref)))
  }

  test("homozygous alt variant, threshold 50") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = ThresholdVariantCaller.callVariantsAtLocus(pileup, 50, emitRef = false)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt)))

    genotypes.length should be(1)
    genotypes.head.variant.position should be(1)
    genotypes.head.variant.referenceAllele.toString should be("T")
    genotypes.head.variant.variantAllele.toString should be("G")
  }

  test("homozygous alt variant, threshold 50; no reference bases observed") {
    val reads = Seq(
      TestUtil.makeRead("TGGATCGA", "8M", "1C6", 1),
      TestUtil.makeRead("TGGATCGA", "8M", "1C6", 1),
      TestUtil.makeRead("TGGATCGA", "8M", "1C6", 1))
    val pileup = Pileup(reads, 2)
    val genotypes = ThresholdVariantCaller.callVariantsAtLocus(pileup, 50, emitRef = false)

    genotypes.length should be(1)
    genotypes.head.variant.position should be(2)
    genotypes.head.variant.referenceAllele.toString should be("C")
    genotypes.head.variant.variantAllele.toString should be("G")

    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt)))
  }
}
