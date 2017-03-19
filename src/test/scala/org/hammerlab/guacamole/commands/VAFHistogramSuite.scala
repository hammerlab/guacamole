package org.hammerlab.guacamole.commands

import org.hammerlab.genomics.readsets.SampleId
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.guacamole.commands.VAFHistogram.generateVAFHistograms
import org.hammerlab.guacamole.util.GuacFunSuite

class VAFHistogramSuite
  extends GuacFunSuite {

  register(
    classOf[Array[VariantLocus]],
    classOf[VariantLocus]
  )

  implicit def convertMap(m: Map[Int, Vector[(Int, Int)]]): Map[SampleId, Vector[(Int, NumLoci)]] =
    m.mapValues(_.map(t ⇒ (t._1, NumLoci(t._2))))

  test("generating the histogram") {

    val loci =
      sc.parallelize(
        Seq(
          VariantLocus(0, "chr1", 1, 0.25f),
          VariantLocus(1, "chr1", 2, 0.35f),
          VariantLocus(0, "chr1", 3, 0.4f),
          VariantLocus(1, "chr1", 4, 0.5f),
          VariantLocus(0, "chr1", 5, 0.55f)
        )
      )

    generateVAFHistograms(loci, 10) should ===(
      Map(
        0 →
          Vector(
            20 → 1,
            40 → 1,
            50 → 1
          ),
        1 →
          Vector(
            30 → 1,
            50 → 1
          )
      )
    )

    generateVAFHistograms(loci, 20) should ===(
      Map(
        0 →
          Vector(
            25 → 1,
            40 → 1,
            55 → 1
          ),
        1 →
          Vector(
            35 → 1,
            50 → 1
          )
      )
    )

    generateVAFHistograms(loci, 100) should ===(
      Map(
        0 →
          Vector(
            25 → 1,
            40 → 1,
            55 → 1
          ),
        1 →
          Vector(
            35 → 1,
            50 → 1
          )
      )
    )
  }
}
