package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole._
import org.scalatest.matchers.ShouldMatchers
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.avro.{ ADAMGenotypeAllele, ADAMGenotype }
import scala.collection.JavaConversions._

class SomaticSubtractionVariantCallerSuite extends SparkFunSuite with ShouldMatchers {

  val thresholdTenCaller = (reads: RDD[MappedRead], loci: LociMap[Long]) =>
    DistributedUtil.pileupFlatMap[ADAMGenotype](
      reads,
      loci,
      pileup => ThresholdVariantCaller.callVariantsAtLocus(pileup, 10, false, false).iterator)

  sparkTest("no variants, same reads with thresholdZeroCaller") {
    val normalReads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1)))

    val tumorReads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1)))

    val genotypes = SomaticSubtractionVariantCaller.callSomaticVariants(thresholdTenCaller, tumorReads, normalReads).collect()
    genotypes.length should be(0)
  }

  sparkTest("variant in tumor but not in normal with thresholdZeroCaller") {
    val normalReads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1)))

    val tumorReads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "0C7", 1),
      TestUtil.makeRead("CCGATCGA", "8M", "0C7", 1),
      TestUtil.makeRead("CCGATCGA", "8M", "0C7", 1)))

    val genotypes = SomaticSubtractionVariantCaller.callSomaticVariants(thresholdTenCaller, tumorReads, normalReads).collect()
    genotypes.length should be(1)

    genotypes.head.alleles.toList should be(List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt))

  }

}
