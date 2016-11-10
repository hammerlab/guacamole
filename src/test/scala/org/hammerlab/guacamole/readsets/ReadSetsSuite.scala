package org.hammerlab.guacamole.readsets

import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.{ADAMContext, ADAMSaveAnyArgs}
import org.hammerlab.guacamole.loci.parsing.ParsedLoci
import org.hammerlab.guacamole.reads.{MappedRead, Read}
import org.hammerlab.guacamole.readsets.io.{Input, InputConfig, TestInputConfig}
import org.hammerlab.guacamole.readsets.rdd.ReadsRDDUtil
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.guacamole.util.TestUtil.resourcePath
import org.hammerlab.magic.test.{LazyAssert, TmpFiles}

class ReadSetsSuite
  extends GuacFunSuite
    with TmpFiles
    with LazyAssert
    with ReadsRDDUtil {

  test("test BAM filtering by loci") {

    def checkFilteredReadCount(loci: String, expectedCount: Int): Unit = {
      withClue(s"filtering to loci: $loci: ") {
        val reads: Array[Read] =
          loadReadsRDD(
            sc,
            "gatk_mini_bundle_extract.bam",
            config = TestInputConfig(ParsedLoci(loci))
          )
          .reads
          .collect

        reads.length should be(expectedCount)
      }
    }

    val lociAndExpectedCounts = Seq(
      "20:9999900"    -> 0,
      "20:9999901"    -> 1,
      "20:9999912"    -> 2,
      "20:0-10000000" -> 9,
      "20:10270532"   -> 1,
      "20:10270533"   -> 0
    )

    for {
      (loci, expectedCount) <- lociAndExpectedCounts
    } {
      checkFilteredReadCount(loci, expectedCount)
    }
  }

  test("reading sam and corresponding bam files should give identical results") {
    def check(bamPath: String, samPath: String, config: InputConfig): Unit =
      withClue(s"using config $config: ") {

        val bamReads =
          loadReadsRDD(
            sc,
            bamPath,
            config
          ).reads.collect

        withClue(s"file $samPath vs $bamPath\n") {

          val samReads =
            loadReadsRDD(
              sc,
              samPath,
              config
            ).reads.collect

          lazyAssert(
            samReads.sameElements(bamReads),
            {
              val missing = bamReads.filter(!samReads.contains(_))
              val extra = samReads.filter(!bamReads.contains(_))
              List(
                "Missing reads:",
                missing.mkString("\t", "\n\t", "\n"),
                "Extra reads:",
                extra.mkString("\t", "\n\t", "\n")
              ).mkString("\n")
            }
          )
        }
      }

    Seq(
      InputConfig.empty,
      TestInputConfig.mapped(nonDuplicate = true),
      TestInputConfig(ParsedLoci("20:10220390-10220490"))
    ).foreach(filter => {
      check("gatk_mini_bundle_extract.bam", "gatk_mini_bundle_extract.sam", filter)
    })

    check(
      "synth1.normal.100k-200k.withmd.bam",
      "synth1.normal.100k-200k.withmd.sam",
      TestInputConfig(ParsedLoci("19:147033"))
    )
  }

  test("load and test config") {
    val allReads = loadReadsRDD(sc, "mdtagissue.sam")
    allReads.reads.count() should be(8)

    val mdTagReads = loadReadsRDD(sc, "mdtagissue.sam", TestInputConfig.mapped())
    mdTagReads.reads.count() should be(5)

    val nonDuplicateReads = loadReadsRDD(
      sc,
      "mdtagissue.sam",
      TestInputConfig.mapped(nonDuplicate = true)
    )
    nonDuplicateReads.reads.count() should be(3)
  }

  test("load RNA reads") {
    val readSet = loadReadsRDD(sc, "rna_chr17_41244936.sam")
    readSet.reads.count should be(23)
  }

  test("load read from ADAM") {
    // First load reads from SAM using ADAM and save as ADAM
    val adamContext = new ADAMContext(sc)
    val adamRecords = adamContext.loadBam(resourcePath("mdtagissue.sam"))

    val adamOut = tmpPath(suffix = ".adam")
    val args = new ADAMSaveAnyArgs {
      override var sortFastqOutput: Boolean = false
      override var asSingleFile: Boolean = true
      override var outputPath: String = adamOut
      override var disableDictionaryEncoding: Boolean = false
      override var blockSize: Int = 1024
      override var pageSize: Int = 1024
      override var compressionCodec: CompressionCodecName = CompressionCodecName.UNCOMPRESSED
    }

    adamRecords.saveAsParquet(args)

    ReadSets.load(adamOut, sc, 0, InputConfig.empty)._1.count() should be(8)

    ReadSets.load(adamOut, sc, 0, TestInputConfig.mapped(nonDuplicate = true))._1.count() should be(3)
  }

  test("load and serialize / deserialize reads") {
    val reads = loadReadsRDD(sc, "mdtagissue.sam", TestInputConfig.mapped()).mappedReads.collect()
    val serializedReads = reads.map(serialize)
    val deserializedReads: Seq[MappedRead] = serializedReads.map(deserialize[MappedRead])
    for ((read, deserialized) <- reads.zip(deserializedReads)) {
      deserialized.contigName should equal(read.contigName)
      deserialized.alignmentQuality should equal(read.alignmentQuality)
      deserialized.start should equal(read.start)
      deserialized.cigar should equal(read.cigar)
      deserialized.failedVendorQualityChecks should equal(read.failedVendorQualityChecks)
      deserialized.isPositiveStrand should equal(read.isPositiveStrand)
      deserialized.isPaired should equal(read.isPaired)
    }
  }

  def sequenceDictionary(records: (String, Int, String)*): SequenceDictionary =
    new SequenceDictionary(
      for {
        (name, length, md5) <- records.toVector
      } yield {
        SequenceRecord(name, length, md5)
      }
    )

  val inputs =
    List(
      Input(0, "f1", "1"),
      Input(1, "f2", "2")
    )

  test("merge identical seqdicts") {
    val dict1 = sequenceDictionary(("1", 111, "aaa"), ("2", 222, "bbb"))
    val dict2 = sequenceDictionary(("1", 111, "aaa"), ("2", 222, "bbb"))
    val merged = ReadSets.mergeSequenceDictionaries(inputs, List(dict1, dict2))
    merged should be(dict1)
    merged should be(dict2)
  }

  test("merge different seqdicts") {
    val dict1 = sequenceDictionary(("1", 111, "aaa"), ("2", 222, "bbb"))
    val dict2 = sequenceDictionary(("1", 111, "aaa"), ("3", 333, "ccc"))

    val merged = ReadSets.mergeSequenceDictionaries(inputs, List(dict1, dict2))

    val expected = sequenceDictionary(("1", 111, "aaa"), ("2", 222, "bbb"), ("3", 333, "ccc"))

    merged should be(expected)
  }

  test("throw on conflicting seqdicts") {
    val dict1 = sequenceDictionary(("1", 111, "aaa"), ("2", 222, "bbb"))
    val dict2 = sequenceDictionary(("1", 111, "aaa"), ("2", 333, "ccc"))

    intercept[IllegalArgumentException] {
      ReadSets.mergeSequenceDictionaries(inputs, List(dict1, dict2))
    }.getMessage should startWith("Conflicting sequence records for 2")
  }
}
