package org.hammerlab.guacamole.readsets

import java.io.{File, IOException}

import htsjdk.samtools.{QueryInterval, SAMRecordIterator, SamReaderFactory, ValidationStringency}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.{ADAMContext, ADAMSpecificRecordSequenceDictionaryRDDAggregator}
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.set.{LociParser, LociSet}
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.reads.{MappedRead, Read}
import org.hammerlab.guacamole.readsets.args.{NoSequenceDictionaryArgs, ReadsArgs, TumorNormalReadsArgs}
import org.hammerlab.guacamole.readsets.loading.{BamReaderAPI, InputFilters, ReadLoadingConfig}
import org.hammerlab.guacamole.readsets.rdd.ReadsRDD
import org.hammerlab.guacamole.readsets.rdd.RegionRDD._
import org.hammerlab.guacamole.reference.{ContigName, Locus}
import org.hammerlab.magic.rdd.RDDStats._
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import org.kohsuke.args4j.{Argument, Option => Args4JOption}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, SAMRecordWritable}

import scala.collection.JavaConversions

/**
 * A `ReadSets` contains reads from multiple inputs, and SequenceDictionary / contig-length information merged from
 * them.
 */
case class ReadSets(readsRDDs: PerSample[ReadsRDD],
                    sequenceDictionary: SequenceDictionary,
                    contigLengths: ContigLengths)
  extends PerSample[ReadsRDD] {

  def numSamples: NumSamples = length
  override def length: NumSamples = readsRDDs.length
  override def apply(sampleId: SampleId): ReadsRDD = readsRDDs(sampleId)

  def sc = readsRDDs.head.reads.sparkContext

  lazy val (mappedReadsRDDs, sourceFiles) = readsRDDs.map(readsRDD => (readsRDD.mappedReads, readsRDD.sourceFile)).unzip

  lazy val allMappedReads = sc.union(mappedReadsRDDs).setName("unioned reads")

  lazy val countStats = allMappedReads.countStats

  implicit lazy val contigLengthsBroadcast = sc.broadcast(contigLengths)

  @transient lazy val allLoci = LociSet.all(contigLengths)
  @transient lazy val allLociBroadcast = sc.broadcast(allLoci)

  def coveragePerSample(halfWindowSize: Int,
                        lociBroadcast: Broadcast[LociSet] = allLociBroadcast): PerSample[RDD[PositionCoverage]] =
    mappedReadsRDDs.map(_.coverage(halfWindowSize, lociBroadcast))

  def totaledCoverage(halfWindowSize: Int): RDD[PositionCoverage] =
    sc
      .union(coveragePerSample((halfWindowSize)))
      .reduceByKey(_ + _)
      .sortByKey()

  def coverage(halfWindowSize: Int, lociBroadcast: Broadcast[LociSet] = allLociBroadcast): RDD[PositionCoverage] =
    allMappedReads.coverage(halfWindowSize, lociBroadcast)

}

object ReadSets {

  /**
   * Common command-line arguments for loading in one or more sets of reads, and associating a sample-name with each.
   */
  trait Arguments extends LociArgs with NoSequenceDictionaryArgs {
    @Argument(required = true, multiValued = true, usage = "FILE1 FILE2 FILE3")
    var paths: Array[String] = Array()

    @Args4JOption(name = "--sample-names", handler = classOf[StringArrayOptionHandler], usage = "name1 ... nameN")
    var sampleNames: Array[String] = Array()

    lazy val pathsAndSampleNames: PerSample[(String, String)] = {
      paths.indices.map(i =>
        paths(i) ->
          (
            if (i < sampleNames.length)
              sampleNames(i)
            else
              paths(i)
          )
      )
    }
  }

  /**
   * Load one read-set from an input file.
   */
  def loadReads(args: ReadsArgs,
                sc: SparkContext,
                filters: InputFilters,
                sampleName: String = "normal"): (ReadsRDD, ContigLengths) = {

    val ReadSets(reads, _, contigLengths) =
      ReadSets(
        sc,
        Seq(args.reads -> sampleName),
        filters,
        contigLengthsFromDictionary = !args.noSequenceDictionary,
        config = ReadLoadingConfig(args)
      )

    (reads(0), contigLengths)
  }

  /**
   * Load just the mapped reads from an input ReadSet.
   */
  def loadMappedReads(args: ReadsArgs,
                      sc: SparkContext,
                      filters: InputFilters): (RDD[MappedRead], ContigLengths) = {
    val (reads, contigLengths) = loadReads(args, sc, filters)
    (reads.mappedReads, contigLengths)
  }

  /**
   * Given arguments for two sets of reads (tumor and normal), return a pair of (tumor, normal) read sets.
   *
   * @param args parsed arguments
   * @param sc spark context
   * @param filters input filters to apply
   */
  def loadTumorNormalReads(args: TumorNormalReadsArgs,
                           sc: SparkContext,
                           filters: InputFilters): (ReadsRDD, ReadsRDD, ContigLengths) = {

    val ReadSets(readsets, _, contigLengths) =
      ReadSets(
        sc,
        Seq(args.tumorReads -> "tumor", args.normalReads -> "normal"),
        filters,
        !args.noSequenceDictionary,
        ReadLoadingConfig(args)
      )

    (readsets(0), readsets(1), contigLengths)
  }

  /**
   * Load ReadSet instances from user-specified BAMs (specified as an InputCollection).
   */
  def apply(sc: SparkContext,
            pathsAndSampleNames: PerSample[(String, String)],
            loci: LociParser,
            contigLengthsFromDictionary: Boolean): ReadSets = {
    ReadSets(
      sc,
      pathsAndSampleNames,
      InputFilters(overlapsLoci = loci),
      contigLengthsFromDictionary = contigLengthsFromDictionary
    )
  }


  def apply(sc: SparkContext, args: Arguments): (ReadSets, LociSet) = {
    val loci = args.parseLoci(sc.hadoopConfiguration)

    val readsets = apply(sc, args.pathsAndSampleNames, loci, !args.noSequenceDictionary)

    (readsets, loci.result(readsets.contigLengths))
  }

  /**
    * Load reads from multiple files, merging their sequence dictionaries and verifying that they are consistent.
    */
  def apply(sc: SparkContext,
            fileAndSampleNames: Seq[(String, String)],
            filters: InputFilters = InputFilters.empty,
            contigLengthsFromDictionary: Boolean = true,
            config: ReadLoadingConfig = ReadLoadingConfig.default): ReadSets = {
    apply(sc, fileAndSampleNames.map((_, filters)), contigLengthsFromDictionary, config)
  }

  /**
   * Load reads from multiple files, allowing different filters to be applied to each file.
   */
  def apply(sc: SparkContext,
            inputs: Seq[((String, String), InputFilters)],
            contigLengthsFromDictionary: Boolean,
            config: ReadLoadingConfig): ReadSets = {

    val (filenamesAndSampleNames, _) = inputs.unzip
    val (filenames, sampleNames) = filenamesAndSampleNames.unzip

    val (readRDDs, sequenceDictionaries) =
      (for {
        (((filename, sampleName), filters), sampleId) <- inputs.zipWithIndex
      } yield
        load(filename, sc, sampleId, filters, config)
      ).unzip

    val sequenceDictionary = mergeSequenceDictionaries(filenames, sequenceDictionaries)

    val contigLengths: ContigLengths = {
      if (contigLengthsFromDictionary) {
        getContigLengthsFromSequenceDictionary(sequenceDictionary)
      } else {
        sc.union(readRDDs)
          .flatMap(_.asMappedRead)
          .map(read => read.contigName -> read.end)
          .reduceByKey(math.max)
          .collectAsMap()
          .toMap
      }
    }

    ReadSets(
      (for {
        ((reads, filename), sampleName) <- readRDDs.zip(filenames).zip(sampleNames)
      } yield
       ReadsRDD(reads, filename, sampleName, contigLengths)
      ).toVector,
      sequenceDictionary,
      contigLengths
    )
  }

  def apply(readsRDDs: PerSample[ReadsRDD], sequenceDictionary: SequenceDictionary): ReadSets =
    ReadSets(
      readsRDDs,
      sequenceDictionary,
      contigLengths(sequenceDictionary)
    )

  /**
   * Given a filename and a spark context, return a pair (RDD, SequenceDictionary), where the first element is an RDD
   * of Reads, and the second element is the Sequence Dictionary giving info (e.g. length) about the contigs in the BAM.
   *
   * @param filename name of file containing reads
   * @param sc spark context
   * @param filters filters to apply
   * @return
   */
  private[readsets] def load(filename: String,
                             sc: SparkContext,
                             sampleId: Int,
                             filters: InputFilters = InputFilters.empty,
                             config: ReadLoadingConfig = ReadLoadingConfig.default): (RDD[Read], SequenceDictionary) = {

    val (allReads, sequenceDictionary) =
      if (filename.endsWith(".bam") || filename.endsWith(".sam"))
        loadFromBAM(filename, sc, sampleId, filters, config)
      else
        loadFromADAM(filename, sc, sampleId, filters, config)

    val reads = filterRDD(allReads, filters, sequenceDictionary)

    (reads, sequenceDictionary)
  }

  /** Returns an RDD of Reads and SequenceDictionary from reads in BAM format **/
  private def loadFromBAM(filename: String,
                          sc: SparkContext,
                          sampleId: Int,
                          filters: InputFilters,
                          config: ReadLoadingConfig): (RDD[Read], SequenceDictionary) = {

    val path = new Path(filename)
    val scheme = path.getFileSystem(sc.hadoopConfiguration).getScheme
    val useSamtools =
      config.bamReaderAPI == BamReaderAPI.Samtools ||
        (
          config.bamReaderAPI == BamReaderAPI.Best &&
          scheme == "file"
        )

    if (useSamtools) {
      // Load with samtools
      if (scheme != "file") {
        throw new IllegalArgumentException(
          "Samtools API can only be used to read local files (i.e. scheme='file'), not: scheme='%s'".format(scheme))
      }
      var requiresFilteringByLocus = filters.overlapsLoci.nonEmpty

      SamReaderFactory.setDefaultValidationStringency(ValidationStringency.SILENT)
      val factory = SamReaderFactory.makeDefault
      val reader = factory.open(new File(path.toUri.getPath))
      val samSequenceDictionary = reader.getFileHeader.getSequenceDictionary
      val sequenceDictionary = SequenceDictionary.fromSAMSequenceDictionary(samSequenceDictionary)
      val loci = filters.overlapsLoci.map(_.result(contigLengths(sequenceDictionary)))

      val recordIterator: SAMRecordIterator =
        if (filters.overlapsLoci.nonEmpty && reader.hasIndex) {
          progress(s"Using samtools with BAM index to read: $filename")
          requiresFilteringByLocus = false
          val queryIntervals = loci.get.contigs.flatMap(contig => {
            val contigIndex = samSequenceDictionary.getSequenceIndex(contig.name)
            contig.ranges.map(range =>
              new QueryInterval(contigIndex,
                range.start.toInt + 1,  // convert 0-indexed inclusive to 1-indexed inclusive
                range.end.toInt))       // "convert" 0-indexed exclusive to 1-indexed inclusive, which is a no-op)
          })
          val optimizedQueryIntervals = QueryInterval.optimizeIntervals(queryIntervals)
          reader.query(optimizedQueryIntervals, false) // Note: this can return unmapped reads, which we filter below.
        } else {
          val skippedReason =
            if (reader.hasIndex)
              "index is available but not needed"
            else
              "index unavailable"

          progress(s"Using samtools without BAM index ($skippedReason) to read: $filename")

          reader.iterator
        }

      val reads =
        JavaConversions.asScalaIterator(recordIterator).flatMap(record => {
          // Optimization: some of the filters are easy to run on the raw SamRecord, so we avoid making a Read.
          if ((filters.overlapsLoci.nonEmpty && record.getReadUnmappedFlag) ||
            (requiresFilteringByLocus &&
              !loci.get.onContig(record.getContig).intersects(record.getStart - 1, record.getEnd)) ||
            (filters.nonDuplicate && record.getDuplicateReadFlag) ||
            (filters.passedVendorQualityChecks && record.getReadFailsVendorQualityCheckFlag) ||
            (filters.isPaired && !record.getReadPairedFlag)) {
            None
          } else {
            val read = Read(record, sampleId)
            assert(filters.overlapsLoci.isEmpty || read.isMapped)
            Some(read)
          }
        })

      (sc.parallelize(reads.toSeq), sequenceDictionary)
    } else {
      // Load with hadoop bam
      progress(s"Using hadoop bam to read: $filename")
      val samHeader = SAMHeaderReader.readSAMHeaderFrom(path, sc.hadoopConfiguration)
      val sequenceDictionary = SequenceDictionary.fromSAMHeader(samHeader)

      val basename = new File(filename).getName.substring(0, 100)

      val reads: RDD[Read] =
        sc
          .newAPIHadoopFile[LongWritable, SAMRecordWritable, AnySAMInputFormat](filename)
          .setName(s"Hadoop file: $basename")
          .values
          .setName(s"Hadoop reads: $basename")
          .map(r => Read(r.get, sampleId))
          .setName(s"Guac reads: $basename")

      (reads, sequenceDictionary)
    }
  }

  /** Returns an RDD of Reads and SequenceDictionary from reads in ADAM format **/
  private def loadFromADAM(filename: String,
                           sc: SparkContext,
                           sampleId: Int,
                           filters: InputFilters,
                           config: ReadLoadingConfig): (RDD[Read], SequenceDictionary) = {

    progress(s"Using ADAM to read: $filename")

    val adamContext = new ADAMContext(sc)

    val adamRecords: RDD[AlignmentRecord] = adamContext.loadAlignments(
      filename, projection = None, stringency = ValidationStringency.LENIENT).rdd

    val sequenceDictionary =
      new ADAMSpecificRecordSequenceDictionaryRDDAggregator(adamRecords).adamGetSequenceDictionary()

    (adamRecords.map(Read(_, sampleId)), sequenceDictionary)
  }


  /** Extract the length of each contig from a sequence dictionary */
  private def contigLengths(sequenceDictionary: SequenceDictionary): ContigLengths = {
    val builder = Map.newBuilder[ContigName, Locus]
    sequenceDictionary.records.foreach(record => builder += ((record.name.toString, record.length)))
    builder.result
  }

  /**
   * SequenceDictionaries store information about the contigs that will be found in a given set of reads: names,
   * lengths, etc.
   *
   * When loading/manipulating multiple sets of reads, we generally want to understand the set of all contigs that
   * are referenced by the reads, perform some consistency-checking (e.g. verifying that each contig is listed as having
   * the same length in each set of reads in which it appears), and finally pass the downstream user a
   * SequenceDictionary that encapsulates all of this.
   *
   * This function performs all of the above.
   *
   * @param filenames Files, each representing a set of reads.
   * @param dicts SequenceDictionaries that have been parsed from @filenames.
   * @return a SequenceDictionary that has been merged and validated from the inputs.
   */
  private[readsets] def mergeSequenceDictionaries(filenames: Seq[String],
                                                  dicts: Seq[SequenceDictionary]): SequenceDictionary = {
    val records =
      (for {
        (filename, dict) <- filenames.zip(dicts)
        record <- dict.records
      } yield {
        filename -> record
      })
      .groupBy(_._2.name)
      .values
      .map(values => {
        val (filename, record) = values.head

        // Verify that all records for a given contig are equal.
        values.tail.toList.filter(_._2 != record) match {
          case Nil => {}
          case mismatched =>
            throw new IllegalArgumentException(
              (
                s"Conflicting sequence records for ${record.name}:" ::
                s"$filename: $record" ::
                mismatched.map { case (otherFile, otherRecord) => s"${otherFile}: $otherRecord" }
              ).mkString("\n\t")
            )
        }

        record
      })

    new SequenceDictionary(records.toVector).sorted
  }

  /**
   * Apply filters to an RDD of reads.
   *
   * @param filters
   * @param reads
   * @param sequenceDictionary
   * @return filtered RDD
   */
  private def filterRDD(reads: RDD[Read], filters: InputFilters, sequenceDictionary: SequenceDictionary): RDD[Read] = {
    /* Note that the InputFilter properties are public, and some loaders directly apply
     * the filters as the reads are loaded, instead of filtering an existing RDD as we do here. If more filters
     * are added, be sure to update those implementations.
     *
     * This is implemented as a static function instead of a method in InputFilters because the overlapsLoci
     * attribute cannot be serialized.
     */
    var result = reads
    if (filters.overlapsLoci.nonEmpty) {
      val loci = filters.overlapsLoci.get.result(contigLengths(sequenceDictionary))
      val broadcastLoci = reads.sparkContext.broadcast(loci)
      result = result.filter(_.asMappedRead.exists(broadcastLoci.value.intersects))
    }
    if (filters.nonDuplicate) result = result.filter(!_.isDuplicate)
    if (filters.passedVendorQualityChecks) result = result.filter(!_.failedVendorQualityChecks)
    if (filters.isPaired) result = result.filter(_.isPaired)
    result
  }

  /**
    * Construct a map from contig name -> length of contig, using a SequenceDictionary.
    */
  private def getContigLengthsFromSequenceDictionary(sequenceDictionary: SequenceDictionary): ContigLengths = {
    val builder = Map.newBuilder[ContigName, Locus]
    for {
      record <- sequenceDictionary.records
    } {
      builder += ((record.name.toString, record.length))
    }
    builder.result
  }

}

case class ReadsNotSortedException(sourceFile: String)
  extends IOException(s"Reads from file $sourceFile are not in sorted order")
