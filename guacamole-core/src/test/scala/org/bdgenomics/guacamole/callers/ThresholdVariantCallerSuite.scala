package org.bdgenomics.guacamole.callers

import org.scalatest.FunSuite
import org.bdgenomics.guacamole.{ TestUtil }
import org.bdgenomics.adam.avro.ADAMGenotypeAllele
import scala.collection.JavaConversions._
import org.bdgenomics.guacamole.pileup.Pileup

class ThresholdVariantCallerSuite extends FunSuite {

  test("no variants, threshold 0") {
    val reads = Seq(
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = ThresholdVariantCaller.callVariantsAtLocus(pileup, 0)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Ref)))
  }

  test("het variant, threshold 0") {
    val reads = Seq(
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("GCGATCGA", "8M", "0G7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = ThresholdVariantCaller.callVariantsAtLocus(pileup, 0)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)))

  }

  test("het variant, threshold 30") {
    val reads = Seq(
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("GCGATCGA", "8M", "0G7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = ThresholdVariantCaller.callVariantsAtLocus(pileup, 30)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)))

  }

  test("het variant, threshold 50, not enough evidence") {
    val reads = Seq(
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("GCGATCGA", "8M", "0G7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = ThresholdVariantCaller.callVariantsAtLocus(pileup, 50)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Ref)))
  }

  test("homozygous alt variant, threshold 50") {
    val reads = Seq(
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("GCGATCGA", "8M", "0G7", 1),
      TestUtil.makeDecadentRead("GCGATCGA", "8M", "0G7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = ThresholdVariantCaller.callVariantsAtLocus(pileup, 50)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt)))
  }

}
