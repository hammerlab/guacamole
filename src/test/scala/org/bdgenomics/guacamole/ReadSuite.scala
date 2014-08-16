/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.guacamole

import org.scalatest.matchers.ShouldMatchers
import net.sf.samtools.TextCigarCodec
import org.bdgenomics.adam.util.MdTag

class ReadSuite extends TestUtil.SparkFunSuite with ShouldMatchers {

  test("mappedread is mapped") {
    val read = MappedRead(
      5, // token
      Bases.stringToBases("TCGACCCTCGA"),
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      "chr5",
      50,
      325352323,
      TextCigarCodec.getSingleton.decode(""),
      None, // mdtag
      false,
      isPositiveStrand = true,
      isPaired = true,
      isFirstInPair = true,
      inferredInsertSize = Some(300),
      isMateMapped = true,
      Some("chr5"),
      Some(100L),
      false)

    read.isMapped should be(true)
    read.asInstanceOf[Read].isMapped should be(true)

    val mappedRead = read.asInstanceOf[Read].getMappedRead()
    mappedRead.isMapped should be(true)

    val collectionMappedReads: Seq[Read] = Seq(read, mappedRead)
    collectionMappedReads(0).isMapped should be(true)
    collectionMappedReads(1).isMapped should be(true)

  }

  test("unmappedread is not mapped") {
    val read = UnmappedRead(
      5, // token
      Bases.stringToBases("TCGACCCTCGA"),
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      false,
      isPositiveStrand = true,
      isPaired = true,
      isFirstInPair = true,
      inferredInsertSize = Some(300),
      isMateMapped = true,
      Some("chr5"),
      Some(100L),
      false)

    read.isMapped should be(false)
    read.asInstanceOf[Read].isMapped should be(false)

    intercept[AssertionError] { read.getMappedRead }

    val collectionMappedReads: Seq[Read] = Seq(read)
    collectionMappedReads(0).isMapped should be(false)
  }

  test("mixed collections mapped and unmapped reads") {
    val uread = UnmappedRead(
      5, // token
      Bases.stringToBases("TCGACCCTCGA"),
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      false,
      isPositiveStrand = true,
      isPaired = true,
      isFirstInPair = true,
      inferredInsertSize = Some(300),
      isMateMapped = true,
      Some("chr5"),
      Some(100L),
      false)

    val mread = MappedRead(
      5, // token
      Bases.stringToBases("TCGACCCTCGA"),
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      "chr5",
      50,
      325352323,
      TextCigarCodec.getSingleton.decode(""),
      None, // mdtag
      false,
      isPositiveStrand = true,
      isPaired = true,
      isFirstInPair = true,
      inferredInsertSize = Some(300),
      isMateMapped = true,
      Some("chr5"),
      Some(100L),
      false)

    val collectionMappedReads: Seq[Read] = Seq(uread, mread)
    collectionMappedReads(0).isMapped should be(false)
    collectionMappedReads(1).isMapped should be(true)
  }

  test("serialize / deserialize mapped read") {
    val read = MappedRead(
      5, // token
      Bases.stringToBases("TCGACCCTCGA"),
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      "chr5",
      50,
      325352323,
      TextCigarCodec.getSingleton.decode(""),
      None, // mdtag
      false,
      isPositiveStrand = true,
      isPaired = true,
      isFirstInPair = true,
      inferredInsertSize = Some(300),
      isMateMapped = true,
      Some("chr5"),
      Some(100L),
      false)

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
    deserialized.isMateMapped should equal(true)
    deserialized.mateReferenceContig should equal(Some("chr5"))
    deserialized.mateStart should equal(Some(100L))
  }

  test("serialize / deserialize mapped read with mdtag") {
    val read = MappedRead(
      5, // token
      Bases.stringToBases("TCGACCCTCGA"),
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      "chr5",
      50,
      325352323,
      TextCigarCodec.getSingleton.decode(""),
      Some("6"), // mdtag
      false,
      isPositiveStrand = true,
      isPaired = true,
      isFirstInPair = true,
      inferredInsertSize = Some(300),
      isMateMapped = true,
      Some("chr5"),
      Some(100L),
      false)

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
    deserialized.isMateMapped should equal(true)
    deserialized.mateReferenceContig should equal(Some("chr5"))
    deserialized.mateStart should equal(Some(100L))
  }

  test("serialize / deserialize mapped read with unmapped pair") {
    val read = MappedRead(
      5, // token
      Bases.stringToBases("TCGACCCTCGA"),
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      "chr5",
      50,
      325352323,
      TextCigarCodec.getSingleton.decode(""),
      None, // mdtag
      false,
      isPositiveStrand = true,
      isPaired = true,
      isFirstInPair = true,
      inferredInsertSize = Some(300),
      isMateMapped = false,
      None,
      None,
      false)

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
    deserialized.isMateMapped should equal(false)
    deserialized.mateReferenceContig should equal(None)
    deserialized.mateStart should equal(None)
  }

  test("serialize / deserialize unmapped read") {
    val read = UnmappedRead(
      22, // token
      Bases.stringToBases("TCGACCCTCGA"),
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      false,
      isPositiveStrand = true,
      isPaired = true,
      isFirstInPair = true,
      inferredInsertSize = Some(300),
      isMateMapped = true,
      Some("chr5"),
      Some(100L),
      false)

    val serialized = TestUtil.serialize(read)
    val deserialized = TestUtil.deserialize[UnmappedRead](serialized)

    // We *should* be able to just use MappedRead's equality implementation, since Scala should implement the equals
    // method for case classes. Somehow, something goes wrong though, and this fails:

    // deserialized should equal(read)

    // So, instead, we'll compare each field ourselves:
    deserialized.token should equal(read.token)
    deserialized.sequence should equal(read.sequence)
    deserialized.baseQualities should equal(read.baseQualities)
    deserialized.isDuplicate should equal(read.isDuplicate)
    deserialized.sampleName should equal(read.sampleName)
    deserialized.failedVendorQualityChecks should equal(read.failedVendorQualityChecks)
    deserialized.isPositiveStrand should equal(read.isPositiveStrand)
    deserialized.isMateMapped should equal(true)
    deserialized.mateReferenceContig should equal(Some("chr5"))
    deserialized.mateStart should equal(Some(100L))
  }

  sparkTest("load and test filters") {
    val allReads = TestUtil.loadReads(sc, "mdtagissue.sam")
    allReads.reads.count() should be(8)

    val mdTagReads = TestUtil.loadReads(sc, "mdtagissue.sam", Read.InputFilters(mapped = true, hasMdTag = true))
    mdTagReads.reads.count() should be(6)

    val nonDuplicateReads = TestUtil.loadReads(sc, "mdtagissue.sam", Read.InputFilters(mapped = true, nonDuplicate = true))
    nonDuplicateReads.reads.count() should be(6)
  }

  sparkTest("load and serialize / deserialize reads") {
    val reads = TestUtil.loadReads(sc, "mdtagissue.sam", Read.InputFilters(mapped = true)).mappedReads.collect()
    val serializedReads = reads.map(TestUtil.serialize)
    val deserializedReads: Seq[MappedRead] = serializedReads.map(TestUtil.deserialize[MappedRead](_))
    for ((read, deserialized) <- reads.zip(deserializedReads)) {
      deserialized.token should equal(read.token)
      deserialized.referenceContig should equal(read.referenceContig)
      deserialized.alignmentQuality should equal(read.alignmentQuality)
      deserialized.start should equal(read.start)
      deserialized.cigar should equal(read.cigar)
      deserialized.mdTagString should equal(read.mdTagString)
      deserialized.failedVendorQualityChecks should equal(read.failedVendorQualityChecks)
      deserialized.isPositiveStrand should equal(read.isPositiveStrand)
      deserialized.isMateMapped should equal(read.isMateMapped)
      deserialized.mateReferenceContig should equal(read.mateReferenceContig)
      deserialized.mateStart should equal(read.mateStart)
    }

  }
}
