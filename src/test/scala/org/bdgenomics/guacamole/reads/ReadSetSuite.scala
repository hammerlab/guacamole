package org.bdgenomics.guacamole.reads

import org.bdgenomics.guacamole.TestUtil
import org.scalatest.Matchers

class ReadSetSuite extends TestUtil.SparkFunSuite with Matchers {

  sparkTest("load and test filters") {
    val allReads = TestUtil.loadReads(sc, "mdtagissue.sam")
    allReads.reads.count() should be(8)

    val mdTagReads = TestUtil.loadReads(sc, "mdtagissue.sam", Read.InputFilters(mapped = true))
    mdTagReads.reads.count() should be(6)

    val nonDuplicateReads = TestUtil.loadReads(sc, "mdtagissue.sam", Read.InputFilters(mapped = true, nonDuplicate = true))
    nonDuplicateReads.reads.count() should be(4)
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
      deserialized.matePropertiesOpt should equal(read.matePropertiesOpt)
    }

  }
}
