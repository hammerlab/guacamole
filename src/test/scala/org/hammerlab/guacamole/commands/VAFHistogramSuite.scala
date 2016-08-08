package org.hammerlab.guacamole.commands

import com.esotericsoftware.kryo.Kryo
import org.hammerlab.guacamole.util.{GuacFunSuite, KryoTestRegistrar}
import org.scalatest.Matchers

class VAFHistogramSuiteRegistrar extends KryoTestRegistrar {
  override def registerTestClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Array[VariantLocus]])
    kryo.register(classOf[VariantLocus])
  }
}

class VAFHistogramSuite extends GuacFunSuite {

  override def registrar = "org.hammerlab.guacamole.commands.VAFHistogramSuiteRegistrar"

  test("generating the histogram") {

    val loci = sc.parallelize(Seq(
      VariantLocus("test", "chr1", 1L, 0.25f),
      VariantLocus("test", "chr1", 2L, 0.35f),
      VariantLocus("test", "chr1", 3L, 0.4f),
      VariantLocus("test", "chr1", 4L, 0.5f),
      VariantLocus("test", "chr1", 5L, 0.55f)
    ))

    val tensHistogram = VAFHistogram.generateVAFHistogram(loci, 10)("test")
    tensHistogram.keys.size should be(4)
    tensHistogram(20) should be(1)
    tensHistogram(30) should be(1)
    tensHistogram(40) should be(1)
    tensHistogram(50) should be(2)

    val fivesHistogram = VAFHistogram.generateVAFHistogram(loci, 20)("test")
    fivesHistogram.keys.size should be(5)
    fivesHistogram(25) should be(1)
    fivesHistogram(35) should be(1)
    fivesHistogram(40) should be(1)
    fivesHistogram(50) should be(1)
    fivesHistogram(55) should be(1)

    val onesHistogram = VAFHistogram.generateVAFHistogram(loci, 100)("test")
    fivesHistogram.keys.size should be(5)
    fivesHistogram(25) should be(1)
    fivesHistogram(35) should be(1)
    fivesHistogram(40) should be(1)
    fivesHistogram(50) should be(1)
    fivesHistogram(55) should be(1)

  }

}
