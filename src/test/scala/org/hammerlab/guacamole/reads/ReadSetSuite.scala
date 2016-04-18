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
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}

class ReadSetSuite extends GuacFunSuite {

  case class LazyMessage(msg: () => String) {
    override def toString: String = msg()
  }

  sparkTest("using different bam reading APIs on sam/bam files should give identical results") {
    def check(paths: Seq[String], filter: InputFilters): Unit = {
      withClue("using filter %s: ".format(filter)) {

        val firstPath = paths.head

        val configs = BamReaderAPI.values.map(ReadLoadingConfig(_))

        val firstConfig = configs.head

        val standard =
          TestUtil.loadReads(
            sc,
            paths.head,
            filter,
            config = configs.head
          ).reads.collect

        for {
          config <- configs
          path <- paths
          if config != firstConfig || path != firstPath
        } {
          withClue(s"file $path with config $config vs standard ${firstPath} with config ${firstConfig}:\n") {

            val result =
              TestUtil.loadReads(
                sc,
                path,
                filter,
                config = config
              ).reads.collect

            assert(
              result.sameElements(standard),
              LazyMessage(
                () => {
                  val missing = standard.filter(!result.contains(_))
                  val extra = result.filter(!standard.contains(_))
                  List(
                    "Missing reads:",
                    missing.mkString("\t", "\n\t", "\n"),
                    "Extra reads:",
                    extra.mkString("\t", "\n\t", "\n")
                  ).mkString("\n")
                }
              )
            )
          }
        }
      }
    }

    Seq(
      InputFilters(),
      InputFilters(mapped = true, nonDuplicate = true),
      InputFilters(overlapsLoci = LociParser("20:10220390-10220490"))
    ).foreach(filter => {
      check(Seq("gatk_mini_bundle_extract.bam", "gatk_mini_bundle_extract.sam"), filter)
    })

    Seq(
      InputFilters(overlapsLoci = LociParser("19:147033"))
    ).foreach(filter => {
      check(Seq("synth1.normal.100k-200k.withmd.bam", "synth1.normal.100k-200k.withmd.sam"), filter)
    })
  }

  sparkTest("load and test filters") {
    val allReads = TestUtil.loadReads(sc, "mdtagissue.sam")
    allReads.reads.count() should be(8)

    val mdTagReads = TestUtil.loadReads(sc, "mdtagissue.sam", InputFilters(mapped = true))
    mdTagReads.reads.count() should be(5)

    val nonDuplicateReads = TestUtil.loadReads(
      sc,
      "mdtagissue.sam",
      InputFilters(mapped = true, nonDuplicate = true)
    )
    nonDuplicateReads.reads.count() should be(3)
  }

  sparkTest("load RNA reads") {
    val readSet = TestUtil.loadReads(sc, "rna_chr17_41244936.sam")
    readSet.reads.count should be(23)
  }

  sparkTest("load read from ADAM") {
    // First load reads from SAM using ADAM and save as ADAM
    val adamContext = new ADAMContext(sc)
    val adamRecords = adamContext.loadBam(TestUtil.testDataPath("mdtagissue.sam"))

    val adamOut = TestUtil.tmpPath(".adam")
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

    val (allReads, _) = Read.loadReadRDDAndSequenceDictionaryFromADAM(adamOut, sc)
    allReads.count() should be(8)

    val (filteredReads, _) = Read.loadReadRDDAndSequenceDictionary(
      adamOut,
      sc,
      InputFilters(mapped = true, nonDuplicate = true)
    )
    filteredReads.count() should be(3)
  }

  sparkTest("load and serialize / deserialize reads") {
    val reads = TestUtil.loadReads(sc, "mdtagissue.sam", InputFilters(mapped = true)).mappedReads.collect()
    val serializedReads = reads.map(TestUtil.serialize)
    val deserializedReads: Seq[MappedRead] = serializedReads.map(TestUtil.deserialize[MappedRead](_))
    for ((read, deserialized) <- reads.zip(deserializedReads)) {
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
