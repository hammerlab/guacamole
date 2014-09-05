package org.bdgenomics.guacamole.reads

import net.sf.samtools.TextCigarCodec
import org.bdgenomics.guacamole.TestUtil
import org.bdgenomics.guacamole.TestUtil.Implicits._
import org.scalatest.Matchers

class MappedReadSuite extends TestUtil.SparkFunSuite with Matchers {

  test("mappedread is mapped") {
    val read = MappedRead(
      5, // token
      "TCGACCCTCGA",
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      "chr5",
      50,
      325352323,
      TextCigarCodec.getSingleton.decode(""),
      mdTagString = "11",
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

    read.isMapped should be(true)
    read.asInstanceOf[Read].isMapped should be(true)

    read.getMappedReadOpt.exists(_.isMapped) should be(true)

  }

  test("mixed collections mapped and unmapped reads") {
    val uread = UnmappedRead(
      5, // token
      "TCGACCCTCGA",
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

    val mread = MappedRead(
      5, // token
      "TCGACCCTCGA",
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      "chr5",
      50,
      325352323,
      TextCigarCodec.getSingleton.decode(""),
      mdTagString = "11",
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

    val collectionMappedReads: Seq[Read] = Seq(uread, mread)
    collectionMappedReads(0).isMapped should be(false)
    collectionMappedReads(1).isMapped should be(true)
  }

}
