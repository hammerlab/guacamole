package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.util.GuacFunSuite
import org.scalatest.Matchers

class VAFHistogramSuite extends GuacFunSuite with Matchers {

  sparkTest("generating the histogram") {

    val loci = sc.parallelize(Seq(
      VariantLocus("chr1", 1L, 0.25f),
      VariantLocus("chr1", 2L, 0.35f),
      VariantLocus("chr1", 3L, 0.4f),
      VariantLocus("chr1", 4L, 0.5f),
      VariantLocus("chr1", 5L, 0.55f)
    ))

    val tensHistogram = VAFHistogram.generateVAFHistogram(loci, 10)
    tensHistogram.keys.size should be(4)
    tensHistogram(20) should be(1)
    tensHistogram(30) should be(1)
    tensHistogram(40) should be(1)
    tensHistogram(50) should be(2)

    val fivesHistogram = VAFHistogram.generateVAFHistogram(loci, 20)
    fivesHistogram.keys.size should be(5)
    fivesHistogram(25) should be(1)
    fivesHistogram(35) should be(1)
    fivesHistogram(40) should be(1)
    fivesHistogram(50) should be(1)
    fivesHistogram(55) should be(1)

    val onesHistogram = VAFHistogram.generateVAFHistogram(loci, 100)
    fivesHistogram.keys.size should be(5)
    fivesHistogram(25) should be(1)
    fivesHistogram(35) should be(1)
    fivesHistogram(40) should be(1)
    fivesHistogram(50) should be(1)
    fivesHistogram(55) should be(1)

  }

}
