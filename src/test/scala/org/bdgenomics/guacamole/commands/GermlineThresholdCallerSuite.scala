package org.bdgenomics.guacamole.commands

import org.scalatest.{ Matchers, FunSuite }
import org.bdgenomics.guacamole.{ TestUtil }
import org.bdgenomics.formats.avro.GenotypeAllele
import scala.collection.JavaConversions._
import org.bdgenomics.guacamole.pileup.Pileup

class GermlineThresholdCallerSuite extends FunSuite with Matchers {

  test("no variants, threshold 0") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = GermlineThresholdCaller.callVariantsAtLocus(pileup, 0)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Ref, GenotypeAllele.Ref)))
  }

  test("het variant, threshold 0") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = GermlineThresholdCaller.callVariantsAtLocus(pileup, 0)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Ref, GenotypeAllele.Alt)))

  }

  test("het variant, threshold 30") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = GermlineThresholdCaller.callVariantsAtLocus(pileup, 30)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Ref, GenotypeAllele.Alt)))

  }

  test("het variant, threshold 50, not enough evidence") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = GermlineThresholdCaller.callVariantsAtLocus(pileup, 50)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Ref, GenotypeAllele.Ref)))
  }

  test("homozygous alt variant, threshold 50") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = GermlineThresholdCaller.callVariantsAtLocus(pileup, 50, emitRef = false)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Alt, GenotypeAllele.Alt)))

    genotypes.length should be(1)
    genotypes.head.getVariant.getStart should be(1)
    genotypes.head.getVariant.getReferenceAllele.toString should be("T")
    genotypes.head.getVariant.getAlternateAllele.toString should be("G")
  }

  test("homozygous alt variant, threshold 50; no reference bases observed") {
    val reads = Seq(
      TestUtil.makeRead("TGGATCGA", "8M", "1C6", 1),
      TestUtil.makeRead("TGGATCGA", "8M", "1C6", 1),
      TestUtil.makeRead("TGGATCGA", "8M", "1C6", 1))
    val pileup = Pileup(reads, 2)
    val genotypes = GermlineThresholdCaller.callVariantsAtLocus(pileup, 50, emitRef = false)

    genotypes.length should be(1)
    genotypes.head.getVariant.getStart should be(2)
    genotypes.head.getVariant.getReferenceAllele.toString should be("C")
    genotypes.head.getVariant.getAlternateAllele.toString should be("G")

    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Alt, GenotypeAllele.Alt)))
  }
}
