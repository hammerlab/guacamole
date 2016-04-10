package org.hammerlab.guacamole.reads

import htsjdk.samtools.TextCigarCodec
import org.hammerlab.guacamole.util.TestUtil
import org.hammerlab.guacamole.util.TestUtil.Implicits._
import org.scalatest.{FunSuite, Matchers}

class PairedReadSuite extends FunSuite with Matchers {

  test("unmappedread paired read is not mapped") {
    val unmappedRead = UnmappedRead(
      "read1",
      "TCGACCCTCGA",
      Array[Byte]((10 to 20).map(_.toByte): _*),
      isDuplicate = true,
      "some sample name",
      failedVendorQualityChecks = false,
      isPaired = true)

    val read =
      PairedRead(
        unmappedRead,
        isFirstInPair = true,
        mateAlignmentProperties = Some(
          MateAlignmentProperties(
            inferredInsertSize = Some(300),
            referenceContig = "chr5",
            start = 100L,
            isPositiveStrand = false
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
        "read1",
        "TCGACCCTCGA",
        Array[Byte]((10 to 20).map(_.toByte): _*),
        isDuplicate = true,
        "some sample name",
        failedVendorQualityChecks = false,
        isPaired = true),
      isFirstInPair = true,
      mateAlignmentProperties = Some(
        MateAlignmentProperties(
          inferredInsertSize = Some(300),
          referenceContig = "chr5",
          start = 100L,
          isPositiveStrand = false
        )
      )
    )

    val mread = PairedRead(
      MappedRead(
        "read1",
        "TCGACCCTCGA",
        Array[Byte]((10 to 20).map(_.toByte): _*),
        isDuplicate = true,
        sampleName = "some sample name",
        referenceContig = "chr5",
        alignmentQuality = 50,
        start = 325352323,
        cigar = TextCigarCodec.decode(""),
        failedVendorQualityChecks = false,
        isPositiveStrand = true,
        isPaired = true),
      isFirstInPair = true,
      mateAlignmentProperties = Some(
        MateAlignmentProperties(
          inferredInsertSize = Some(300),
          referenceContig = "chr5",
          start = 100L,
          isPositiveStrand = false
        )
      )
    )

    val collectionMappedReads: Seq[Read] = Seq(uread, mread)
    collectionMappedReads(0).isMapped should be(false)
    collectionMappedReads(1).isMapped should be(true)
  }

}
