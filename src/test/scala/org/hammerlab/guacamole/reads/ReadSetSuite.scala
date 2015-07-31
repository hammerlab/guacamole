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

package org.hammerlab.guacamole.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.guacamole.util.{ TestUtil, GuacFunSuite }
import org.scalatest.Matchers
import org.bdgenomics.adam.rdd.ADAMContext._

class ReadSetSuite extends GuacFunSuite with Matchers {

  sparkTest("load and test filters") {
    val allReads = TestUtil.loadReads(sc, "mdtagissue.sam")
    allReads.reads.count() should be(8)

    val mdTagReads = TestUtil.loadReads(sc, "mdtagissue.sam", Read.InputFilters(mapped = true))
    mdTagReads.reads.count() should be(6)

    val nonDuplicateReads = TestUtil.loadReads(
      sc,
      "mdtagissue.sam",
      Read.InputFilters(mapped = true, nonDuplicate = true))
    nonDuplicateReads.reads.count() should be(4)
  }

  sparkTest("load RNA reads") {
    val readSet = TestUtil.loadReads(sc, "rna_chr17_41244936.sam")
    readSet.reads.count should be(23)
  }

  sparkTest("load read from ADAM") {
    // First load reads from SAM using ADAM and save as ADAM
    val adamContext = new ADAMContext(sc)
    val adamAlignmentRecords: RDD[AlignmentRecord] = adamContext.loadAlignments(TestUtil.testDataPath("mdtagissue.sam"))
    val origReads = TestUtil.loadReads(sc, "mdtagissue.sam").reads.collect()

    val adamOut = TestUtil.tmpFileName(".adam")
    adamAlignmentRecords.adamParquetSave(adamOut)

    val (allReads, _) = Read.loadReadRDDAndSequenceDictionaryFromADAM(adamOut, sc, token = 0)
    allReads.count() should be(8)
    val collectedReads = allReads.collect()

    val (filteredReads, _) = Read.loadReadRDDAndSequenceDictionary(
      adamOut,
      sc,
      token = 1,
      Read.InputFilters(mapped = true, nonDuplicate = true),
      requireMDTagsOnMappedReads = true)
    filteredReads.count() should be(4)
    filteredReads.collect().forall(_.token == 1) should be(true)

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
      deserialized.isPaired should equal(read.isPaired)
    }

  }

  sparkTest("load mdTaggedReads") {
    val readSet = TestUtil.loadReads(sc, "different_start_reads.sam")
    readSet.mappedReads.collect().length should be(8)
    readSet.mdTaggedReads.collect().length should be(8)
    readSet.mappedPairedReads.collect().length should be(8)
    readSet.mdTaggedPairedReads.collect().length should be(8)
  }

  sparkTest("load without mdTaggedReads") {
    val readSet = TestUtil.loadReads(sc, "tumor_without_mdtag.sam")
    readSet.reads.collect().length should be(50)
    readSet.mappedReads.collect().length should be(50)
    readSet.mdTaggedReads.collect().length should be(0)
    readSet.mappedPairedReads.collect().length should be(50)
    readSet.mdTaggedPairedReads.collect().length should be(0)
  }
}
