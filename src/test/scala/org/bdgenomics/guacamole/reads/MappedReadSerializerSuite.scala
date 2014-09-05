package org.bdgenomics.guacamole.reads

import net.sf.samtools.TextCigarCodec
import org.bdgenomics.guacamole.TestUtil
import org.bdgenomics.guacamole.TestUtil.Implicits._
import org.scalatest.Matchers

class MappedReadSerializerSuite extends TestUtil.SparkFunSuite with Matchers {

  test("serialize / deserialize mapped read") {
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

    val serialized = TestUtil.serialize(read)
    val deserialized = TestUtil.deserialize[MappedRead](serialized)

    // We *should* be able to just use MappedRead's equality implementation, since Scala should implement the equals
    // method for case classes. Somehow, something goes wrong though, and this fails:

    // deserialized should equal(read)

    // So, instead, we'll compare each field ourselves:
    deserialized.token should equal(read.token)
    deserialized.sequence should equal(read.sequence)
    deserialized.baseQualities should equal(read.baseQualities)
    deserialized.isDuplicate should equal(read.isDuplicate)
    deserialized.sampleName should equal(read.sampleName)
    deserialized.referenceContig should equal(read.referenceContig)
    deserialized.alignmentQuality should equal(read.alignmentQuality)
    deserialized.start should equal(read.start)
    deserialized.cigar should equal(read.cigar)
    deserialized.mdTag should equal(read.mdTag)
    deserialized.failedVendorQualityChecks should equal(read.failedVendorQualityChecks)
    deserialized.isPositiveStrand should equal(read.isPositiveStrand)
    deserialized.matePropertiesOpt should equal(read.matePropertiesOpt)
  }

  test("serialize / deserialize mapped read with mdtag") {
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

    val serialized = TestUtil.serialize(read)
    val deserialized = TestUtil.deserialize[MappedRead](serialized)

    // We *should* be able to just use MappedRead's equality implementation, since Scala should implement the equals
    // method for case classes. Somehow, something goes wrong though, and this fails:

    // deserialized should equal(read)

    // So, instead, we'll compare each field ourselves:
    deserialized.token should equal(read.token)
    deserialized.sequence should equal(read.sequence)
    deserialized.baseQualities should equal(read.baseQualities)
    deserialized.isDuplicate should equal(read.isDuplicate)
    deserialized.sampleName should equal(read.sampleName)
    deserialized.referenceContig should equal(read.referenceContig)
    deserialized.alignmentQuality should equal(read.alignmentQuality)
    deserialized.start should equal(read.start)
    deserialized.cigar should equal(read.cigar)
    deserialized.mdTag should equal(read.mdTag)
    deserialized.failedVendorQualityChecks should equal(read.failedVendorQualityChecks)
    deserialized.isPositiveStrand should equal(read.isPositiveStrand)
    deserialized.matePropertiesOpt should equal(read.matePropertiesOpt)
  }

  test("serialize / deserialize mapped read with unmapped pair") {
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
          isMateMapped = false,
          None,
          None,
          false
        )
      )
    )

    val serialized = TestUtil.serialize(read)
    val deserialized = TestUtil.deserialize[MappedRead](serialized)

    // We *should* be able to just use MappedRead's equality implementation, since Scala should implement the equals
    // method for case classes. Somehow, something goes wrong though, and this fails:

    // deserialized should equal(read)

    // So, instead, we'll compare each field ourselves:
    deserialized.token should equal(read.token)
    deserialized.sequence should equal(read.sequence)
    deserialized.baseQualities should equal(read.baseQualities)
    deserialized.isDuplicate should equal(read.isDuplicate)
    deserialized.sampleName should equal(read.sampleName)
    deserialized.referenceContig should equal(read.referenceContig)
    deserialized.alignmentQuality should equal(read.alignmentQuality)
    deserialized.start should equal(read.start)
    deserialized.cigar should equal(read.cigar)
    deserialized.mdTag should equal(read.mdTag)
    deserialized.failedVendorQualityChecks should equal(read.failedVendorQualityChecks)
    deserialized.isPositiveStrand should equal(read.isPositiveStrand)
    deserialized.matePropertiesOpt should equal(read.matePropertiesOpt)
  }

}
