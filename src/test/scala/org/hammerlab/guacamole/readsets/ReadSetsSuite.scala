package org.hammerlab.guacamole.readsets

import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDDFunctions
import org.bdgenomics.adam.rdd.{ADAMContext, ADAMSaveAnyArgs}
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.io.{BamReaderAPI, Input, InputFilters, ReadLoadingConfig}
import org.hammerlab.guacamole.readsets.rdd.ReadsRDDUtil
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}

class ReadSetsSuite
  extends GuacFunSuite
    with ReadsRDDUtil {

  case class LazyMessage(msg: () => String) {
    override def toString: String = msg()
  }

  test("using different bam reading APIs on sam/bam files should give identical results") {
    def check(paths: Seq[String], filter: InputFilters): Unit = {
      withClue("using filter %s: ".format(filter)) {

        val firstPath = paths.head

        val configs = BamReaderAPI.values.map(ReadLoadingConfig(_))

        val firstConfig = configs.head

        val standard =
          loadReadsRDD(
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
          withClue(s"file $path with config $config vs standard $firstPath with config $firstConfig:\n") {

            val result =
              loadReadsRDD(
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

  test("load and test filters") {
    val allReads = loadReadsRDD(sc, "mdtagissue.sam")
    allReads.reads.count() should be(8)

    val mdTagReads = loadReadsRDD(sc, "mdtagissue.sam", InputFilters(mapped = true))
    mdTagReads.reads.count() should be(5)

    val nonDuplicateReads = loadReadsRDD(
      sc,
      "mdtagissue.sam",
      InputFilters(mapped = true, nonDuplicate = true)
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

    ReadSets.load(adamOut, sc, InputFilters.empty)._1.count() should be(8)

    ReadSets.load(adamOut, sc, InputFilters(mapped = true, nonDuplicate = true))._1.count() should be(3)
  }

  test("load and serialize / deserialize reads") {
    val reads = loadReadsRDD(sc, "mdtagissue.sam", InputFilters(mapped = true)).mappedReads.collect()
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
      Input("f1", "1"),
      Input("f2", "2")
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
