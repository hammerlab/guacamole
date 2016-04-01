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

import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDDFunctions
import org.bdgenomics.adam.rdd.{ADAMContext, ADAMSaveAnyArgs}
import org.hammerlab.guacamole.LociSet
import org.hammerlab.guacamole.reads.Read.InputFilters
import org.hammerlab.guacamole.reference.ReferenceGenome
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}
import org.scalatest.Matchers

class ReadSetSuite extends GuacFunSuite with Matchers {

  def chr22Fasta = ReferenceGenome.readFasta(TestUtil.testDataPath("chr22.fa.gz"))

  sparkTest("using different bam reading APIs on sam/bam files should give identical results") {
    def check(paths: Seq[String], filter: InputFilters): Unit = {
      withClue("using filter %s: ".format(filter)) {
        val configs = Read.ReadLoadingConfig.BamReaderAPI.values.map(api => Read.ReadLoadingConfig(bamReaderAPI = api)).toSeq
        val standard = TestUtil.loadReads(sc, paths(0), filter, config = configs(0), reference = chr22Fasta).reads.collect
        configs.foreach(config => {
          paths.foreach(path => {
            withClue("file %s with config %s vs standard %s with config %s:\n".format(path, config, paths(0), configs(0))) {
              val result = TestUtil.loadReads(sc, path, filter, config = config, reference = chr22Fasta).reads.collect
              if (result.toSet != standard.toSet) {
                val missing = standard.filter(!result.contains(_))
                assert(missing.isEmpty, "Missing reads: %s".format(missing.map(_.toString).mkString("\n\t")))
                val extra = result.filter(!standard.contains(_))
                assert(extra.isEmpty, "Extra reads:\n\t%s".format(extra.map(_.toString).mkString("\n\t")))
                assert(false, "shouldn't get here")
              }
            }
          })
        })
      }
    }

    Seq(
      InputFilters(),
      InputFilters(mapped = true, nonDuplicate = true),
      InputFilters(overlapsLoci = Some(LociSet.parse("20:10220390-10220490")))
    ).foreach(filter => {
        check(Seq("gatk_mini_bundle_extract.bam", "gatk_mini_bundle_extract.sam"), filter)
      })

    Seq(
      InputFilters(overlapsLoci = Some(LociSet.parse("19:147033-147034")))
    ).foreach(filter => {
        check(Seq("synth1.normal.100k-200k.withmd.bam", "synth1.normal.100k-200k.withmd.sam"), filter)
      })
  }

  sparkTest("load and test filters") {
    val allReads = TestUtil.loadReads(sc, "mdtagissue.sam", reference = chr22Fasta)
    allReads.reads.count() should be(8)

    val mdTagReads = TestUtil.loadReads(sc, "mdtagissue.sam", Read.InputFilters(mapped = true), reference = chr22Fasta)
    mdTagReads.reads.count() should be(5)

    val nonDuplicateReads = TestUtil.loadReads(
      sc,
      "mdtagissue.sam",
      Read.InputFilters(mapped = true, nonDuplicate = true),
      reference = chr22Fasta)
    nonDuplicateReads.reads.count() should be(3)
  }

  sparkTest("load RNA reads") {
    val chr17Reference = TestUtil.makeReference(Seq(("chr17", 0, "")), 412449360)
    val readSet = TestUtil.loadReads(sc, "rna_chr17_41244936.sam", reference = chr17Reference)
    readSet.reads.count should be(23)
  }

  sparkTest("load read from ADAM") {
    // First load reads from SAM using ADAM and save as ADAM
    val adamContext = new ADAMContext(sc)
    val adamRecords = adamContext.loadBam(TestUtil.testDataPath("mdtagissue.sam"))

    val origReadSet = TestUtil.loadReads(sc, "mdtagissue.sam", reference = chr22Fasta)

    val adamOut = TestUtil.tmpFileName(".adam")
    val args = new ADAMSaveAnyArgs {
      override var sortFastqOutput: Boolean = false
      override var asSingleFile: Boolean = true
      override var outputPath: String = adamOut
      override var disableDictionaryEncoding: Boolean = false
      override var blockSize: Int = 1024
      override var pageSize: Int = 1024
      override var compressionCodec: CompressionCodecName = CompressionCodecName.UNCOMPRESSED
    }
    new AlignmentRecordRDDFunctions(adamRecords.rdd).saveAsParquet(args, adamRecords.sequences, adamRecords.recordGroups)

    val (allReads, _) = Read.loadReadRDDAndSequenceDictionaryFromADAM(adamOut, sc, token = 0, reference = chr22Fasta)
    allReads.count() should be(8)
    val collectedReads = allReads.collect()

    val (filteredReads, _) = Read.loadReadRDDAndSequenceDictionary(
      adamOut,
      sc,
      1,
      Read.InputFilters(mapped = true, nonDuplicate = true),
      reference = chr22Fasta)
    filteredReads.count() should be(3)
    filteredReads.collect().forall(_.token == 1) should be(true)
  }

  sparkTest("load and serialize / deserialize reads") {
    val chr17Reference = TestUtil.makeReference(Seq(("chr17", 0, "")), 412449360)
    val reads = TestUtil.loadReads(sc, "mdtagissue.sam", Read.InputFilters(mapped = true), reference = chr17Reference).mappedReads.collect()
    val serializedReads = reads.map(TestUtil.serialize)
    val deserializedReads: Seq[MappedRead] = serializedReads.map(TestUtil.deserialize[MappedRead](_))
    for ((read, deserialized) <- reads.zip(deserializedReads)) {
      deserialized.token should equal(read.token)
      deserialized.referenceContig should equal(read.referenceContig)
      deserialized.alignmentQuality should equal(read.alignmentQuality)
      deserialized.start should equal(read.start)
      deserialized.cigar should equal(read.cigar)
      deserialized.failedVendorQualityChecks should equal(read.failedVendorQualityChecks)
      deserialized.isPositiveStrand should equal(read.isPositiveStrand)
      deserialized.isPaired should equal(read.isPaired)
    }
  }
}
