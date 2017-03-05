package org.hammerlab.guacamole.reads

import htsjdk.samtools.TextCigarCodec
import org.hammerlab.guacamole.reference.ReferenceUtil
import org.hammerlab.guacamole.util.GuacFunSuite

class MappedReadSuite
  extends GuacFunSuite
    with ReadsUtil
    with ReferenceUtil {

  test("mappedread is mapped") {
    val read = MappedRead(
      "read1",
      "TCGACCCTCGA",
      Array[Byte]((10 to 20).map(_.toByte): _*),
      isDuplicate = true,
      sampleId = 123,
      "chr5",
      50,
      325352323,
      TextCigarCodec.decode(""),
      failedVendorQualityChecks = false,
      isPositiveStrand = true,
      isPaired = true
    )

    read.isMapped should be(true)
    read.asInstanceOf[Read].isMapped should be(true)
  }

  test("mixed collections mapped and unmapped reads") {
    val uread = UnmappedRead(
      "read1",
      "TCGACCCTCGA",
      Array[Byte]((10 to 20).map(_.toByte): _*),
      isDuplicate = true,
      123,
      failedVendorQualityChecks = false,
      isPaired = true
    )

    val mread = MappedRead(
      "read1",
      "TCGACCCTCGA",
      Array[Byte]((10 to 20).map(_.toByte): _*),
      isDuplicate = true,
      123,
      "chr5",
      50,
      325352323,
      TextCigarCodec.decode(""),
      failedVendorQualityChecks = false,
      isPositiveStrand = true,
      isPaired = true
    )

    val collectionMappedReads: Seq[Read] = Seq(uread, mread)
    collectionMappedReads(0).isMapped should be(false)
    collectionMappedReads(1).isMapped should be(true)
  }

  lazy val reference = makeReference(sc, "chr1", 8, "GGTCGATCGATCAA")

  test("slice read matching read") {
    val chr1Contig = reference.getContig("chr1")
    val readLength = 10
    val qualityScores = (0 until readLength).map( _ => 30)

    val simpleRead = makeRead(
      "TCGATCGATC",
      start = 10,
      cigar = "10M",
      qualityScores = Some(qualityScores)
    )

    val sliceAll = simpleRead.slice(10L, 20L, chr1Contig).get
    sliceAll should be(simpleRead)

    val sliceNone = simpleRead.slice(20L, 30L, chr1Contig)
    sliceNone should be (None)

    val sliceFirstFive = simpleRead.slice(10L, 15L, chr1Contig).get
    sliceFirstFive.start should === (10L)
    assert(sliceFirstFive.sequence === "TCGAT")
    sliceFirstFive.cigar.toString should be ("5M")
    sliceFirstFive.end should === (15L)

    val sliceLastFive = simpleRead.slice(15L, 20L, chr1Contig).get
    sliceLastFive.start should === (15L)
    assert(sliceLastFive.sequence === "CGATC")
    sliceLastFive.cigar.toString should be ("5M")
    sliceLastFive.end should === (20L)
  }

  test("slice read with deletion") {
    val chr1Contig = reference.getContig("chr1")
    val readLength = 10
    val qualityScores = (0 until readLength).map( _ => 30)

    val deletionRead = makeRead(
      "GGTCGATCAA",
      start = 8,
      cigar = "6M4D4M",
      qualityScores = Some(qualityScores)
    )

    val sliceBeforeDeletion = deletionRead.slice(11L, 20L, chr1Contig).get
    sliceBeforeDeletion.start should === (11L)
    assert(sliceBeforeDeletion.sequence === "CGATC")
    sliceBeforeDeletion.cigar.toString should be ("3M4D2M")
    sliceBeforeDeletion.end should === (20L)

    val sliceInDeletion = deletionRead.slice(16L, 20L, chr1Contig).get
    sliceInDeletion.start should === (16L)
    assert(sliceInDeletion.sequence === "TC")
    sliceInDeletion.cigar.toString should be ("2D2M")
    sliceInDeletion.end should === (20L)
  }

  test("slice read with insertion") {
    val chr1Contig = reference.getContig("chr1")
    val readLength = 15
    val qualityScores = (0 until readLength).map( _ => 30)

    val insertionRead = makeRead(
      "TCGACCCCCTCGATC",
      start = 10,
      cigar = "4M5I6M",
      qualityScores = Some(qualityScores)
    )

    val sliceBeforeInsertion = insertionRead.slice(12L, 18L, chr1Contig).get
    sliceBeforeInsertion.start should === (12L)
    assert(sliceBeforeInsertion.sequence === "GACCCCCTCGA")
    sliceBeforeInsertion.cigar.toString should be ("2M5I4M")
    sliceBeforeInsertion.end should === (18L)

    val sliceAfterInsertion = insertionRead.slice(14L, 18L, chr1Contig).get
    sliceAfterInsertion.start should === (14L)
    assert(sliceAfterInsertion.sequence === "TCGA")
    sliceAfterInsertion.cigar.toString should be ("4M")
    sliceAfterInsertion.end should === (18L)
  }

}
