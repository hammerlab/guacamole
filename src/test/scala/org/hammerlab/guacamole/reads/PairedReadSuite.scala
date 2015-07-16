package org.hammerlab.guacamole.reads

import htsjdk.samtools.TextCigarCodec
import org.hammerlab.guacamole.util.TestUtil.Implicits._
import org.scalatest.{ Matchers, FunSuite }

class PairedReadSuite extends FunSuite with Matchers {

  test("unmappedread paired read is not mapped") {
    val unmappedRead = UnmappedRead(
      5, // token
      "TCGACCCTCGA",
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      false,
      isPositiveStrand = true,
      isPaired = true)

    val read =
      PairedRead(
        unmappedRead,
        isFirstInPair = true,
        mateAlignmentProperties = Some(
          MateAlignmentProperties(
            inferredInsertSize = Some(300),
            mateReferenceContig = "chr5",
            mateStart = 100L,
            isMatePositiveStrand = false
          )
        )
      )

    read.isMapped should be(false)
    read.asInstanceOf[Read].isMapped should be(false)

    val collectionMappedReads: Seq[Read] = Seq(read)
    collectionMappedReads(0).isMapped should be(false)
  }

  test("mixed collections mapped and unmapped read pairs") {
    val uread = PairedRead(
      UnmappedRead(
        5, // token
        "TCGACCCTCGA",
        Array[Byte]((10 to 20).map(_.toByte): _*),
        true,
        "some sample name",
        false,
        isPositiveStrand = true,
        isPaired = true),
      isFirstInPair = true,
      mateAlignmentProperties = Some(
        MateAlignmentProperties(
          inferredInsertSize = Some(300),
          mateReferenceContig = "chr5",
          mateStart = 100L,
          isMatePositiveStrand = false
        )
      )
    )

    val mread = PairedRead(
      MappedRead(
        5, // token
        "TCGACCCTCGA",
        Array[Byte]((10 to 20).map(_.toByte): _*),
        true,
        "some sample name",
        "chr5",
        50,
        325352323,
        TextCigarCodec.decode(""),
        mdTagString = "11",
        false,
        isPositiveStrand = true,
        isPaired = true),
      isFirstInPair = true,
      mateAlignmentProperties = Some(
        MateAlignmentProperties(
          inferredInsertSize = Some(300),
          mateReferenceContig = "chr5",
          mateStart = 100L,
          isMatePositiveStrand = false
        )
      )
    )

    val collectionMappedReads: Seq[Read] = Seq(uread, mread)
    collectionMappedReads(0).isMapped should be(false)
    collectionMappedReads(1).isMapped should be(true)
  }

}
