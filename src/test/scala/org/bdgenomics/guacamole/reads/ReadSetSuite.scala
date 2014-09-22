package org.bdgenomics.guacamole.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.{ DistributedUtil, Common, ReadSet, TestUtil }
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

  sparkTest("compute read statistics") {
    val reads: RDD[Read] = sc.parallelize((0 to 100).map(i => TestUtil.makePairedRead(start = i,
      sequence = (0 to 25).map(i => "TCGA").mkString,
      cigar = "100M",
      mdTag = "100"

    )))
    val readSet = ReadSet(reads,
      sequenceDictionary = None,
      source = "test",
      filters = Read.InputFilters(),
      token = 0,
      contigLengthsFromDictionary = false)

    val loci = Common.loci("chr1:0-99", readSet)
    val lociPartitions = DistributedUtil.partitionLociUniformly(1, loci)
    val metrics = readSet.libraryMetrics(lociPartitions)

    metrics.numLoci should be(99)
    metrics.numReads should be(99)
    TestUtil.assertAlmostEqual(metrics.averageReadDepth, 50)
    TestUtil.assertAlmostEqual(metrics.averageInsertSize, 500)
  }
}
