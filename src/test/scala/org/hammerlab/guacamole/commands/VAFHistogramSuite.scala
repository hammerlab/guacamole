package org.hammerlab.guacamole.commands

import com.esotericsoftware.kryo.Kryo
import org.hammerlab.guacamole.util.{GuacFunSuite, KryoTestRegistrar}

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
      VariantLocus(0, "chr1", 1L, 0.25f),
      VariantLocus(1, "chr1", 2L, 0.35f),
      VariantLocus(0, "chr1", 3L, 0.4f),
      VariantLocus(1, "chr1", 4L, 0.5f),
      VariantLocus(0, "chr1", 5L, 0.55f)
    ))

    VAFHistogram.generateVAFHistograms(loci, 10) should be(
      Map(
        0 ->
          Vector(
            20 -> 1,
            40 -> 1,
            50 -> 1
          ),
        1 ->
          Vector(
            30 -> 1,
            50 -> 1
          )
      )
    )

    VAFHistogram.generateVAFHistograms(loci, 20) should be(
      Map(
        0 ->
          Vector(
            25 -> 1,
            40 -> 1,
            55 -> 1
          ),
        1 ->
          Vector(
            35 -> 1,
            50 -> 1
          )
      )
    )

    VAFHistogram.generateVAFHistograms(loci, 100) should be(
      Map(
        0 ->
          Vector(
            25 -> 1,
            40 -> 1,
            55 -> 1
          ),
        1 ->
          Vector(
            35 -> 1,
            50 -> 1
          )
      )
    )
  }
}
