package org.bdgenomics.guacamole.reads

import org.bdgenomics.guacamole.{ Bases, TestUtil }
import org.scalatest.Matchers

class UnmappedReadSuite extends TestUtil.SparkFunSuite with Matchers {

  test("unmappedread is not mapped") {
    val read = UnmappedRead(
      5, // token
      Bases.stringToBases("TCGACCCTCGA"),
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      false,
      isPositiveStrand = true,
      matePropertiesOpt = Some(
        MateProperties(
          isFirstInPair = true,
          inferredInsertSize = Some(300),
          isMateMapped = true,
          Some("chr5"),
          Some(100L),
          false
        )
      )
    )

    read.isMapped should be(false)
    read.asInstanceOf[Read].isMapped should be(false)
    read.getMappedReadOpt should be(None)

    val collectionMappedReads: Seq[Read] = Seq(read)
    collectionMappedReads(0).isMapped should be(false)
  }

}
