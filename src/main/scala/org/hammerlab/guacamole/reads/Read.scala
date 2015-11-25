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

import java.io.File

import htsjdk.samtools._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.projections.{ AlignmentRecordField, Projection }
import org.bdgenomics.adam.rdd.{ ADAMContext, ADAMSpecificRecordSequenceDictionaryRDDAggregator }
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.guacamole.reads.Read.BamReaderAPI.BamReaderAPI
import org.hammerlab.guacamole.{ LociSet, Bases }
import org.hammerlab.guacamole.reference.ReferenceGenome
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{ AnySAMInputFormat, SAMRecordWritable }

import scala.collection.JavaConversions

/**
 * The fields in the Read trait are common to any read, whether mapped (aligned) or not.
 */
trait Read {
  /**
   * Each read has a "token", which is an arbitrary integer for use by the application. Unlike the other fields,
   * a read's token does NOT correspond to any underlying column in e.g. the BAM file.
   *
   * It's used, for example, to differentiate tumor/normal pairs in some somatic callers.
   *
   * This field can be set when reading in the reads, or modified at any point by the application.
   *
   * Many applications just ignore this.
   *
   */
  def token: Int

  /** The nucleotide sequence. */
  def sequence: Seq[Byte]

  /** The base qualities, phred scaled.  These are numbers, and are NOT character encoded. */
  def baseQualities: Seq[Byte]

  /** Is this read a duplicate of another? */
  def isDuplicate: Boolean

  /** Is this read mapped? */
  def isMapped: Boolean

  def asMappedRead: Option[MappedRead]

  /** The sample (e.g. "tumor" or "patient3636") name. */
  def sampleName: String

  /** Whether the read failed predefined vendor checks for quality */
  def failedVendorQualityChecks: Boolean

  /** Whether read is from a paired-end library */
  def isPaired: Boolean

  /** Whether the read has associated reference data */
  def hasMdTag: Boolean

}

object Read extends Logging {
  /**
   * Filtering reads while they are loaded can be an important optimization.
   *
   * These fields are commonly used filters. Setting a field to True will result in filtering on that field. If multiple
   * fields are set, the result is the intersection of the filters (i.e. reads must satisfy ALL filters).
   *
   * @param mapped include only mapped reads
   * @param nonDuplicate include only reads that do not have the duplicate bit set
   * @param passedVendorQualityChecks include only reads that do not have the failedVendorQualityChecks bit set
   * @param isPaired include only reads are paired-end reads
   */
  class InputFilters(
      val overlapsLoci: Option[LociSet.Builder] = None,
      val nonDuplicate: Boolean = false,
      val passedVendorQualityChecks: Boolean = false,
      val isPaired: Boolean = false,
      val hasMdTag: Boolean = false) {
  }
  object InputFilters {
    val empty = InputFilters()

    def apply(): InputFilters = new InputFilters()
    def apply(overlapsLoci: Option[LociSet.Builder] = None,
              mapped: Boolean = false,
              nonDuplicate: Boolean = false,
              passedVendorQualityChecks: Boolean = false,
              isPaired: Boolean = false,
              hasMdTag: Boolean = false): InputFilters = {
      new InputFilters(
        overlapsLoci = if (overlapsLoci.isEmpty && mapped) Some(LociSet.newBuilder.putAllContigs) else overlapsLoci,
        nonDuplicate = nonDuplicate,
        passedVendorQualityChecks = passedVendorQualityChecks,
        isPaired = isPaired,
        hasMdTag = hasMdTag)
    }

    def filterRDD(filters: InputFilters, reads: RDD[Read], sequenceDictionary: SequenceDictionary): RDD[Read] = {
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
        result = result.filter(
          read => read.isMapped && read.asMappedRead.get.overlapsLociSet(broadcastLoci.value))
      }
      if (filters.nonDuplicate) result = result.filter(!_.isDuplicate)
      if (filters.passedVendorQualityChecks) result = result.filter(!_.failedVendorQualityChecks)
      if (filters.isPaired) result = result.filter(_.isPaired)
      if (filters.hasMdTag) result = result.filter(_.hasMdTag)
      result
    }
  }

  /**
   * Convenience function (intended for test cases), to construct a Read from unparsed values.
   */
  def apply(
    sequence: String,
    token: Int = 0,
    baseQualities: String = "",
    isDuplicate: Boolean = false,
    sampleName: String = "",
    referenceContig: String = "",
    alignmentQuality: Int = -1,
    start: Long = -1L,
    cigarString: String = "",
    mdTagString: Option[String],
    failedVendorQualityChecks: Boolean = false,
    isPositiveStrand: Boolean = true,
    isPaired: Boolean = true) = {

    val sequenceArray = sequence.map(_.toByte).toArray
    val qualityScoresArray = baseQualityStringToArray(baseQualities, sequenceArray.length)

    val cigar = TextCigarCodec.decode(cigarString)
    MappedRead(
      token,
      sequenceArray,
      qualityScoresArray,
      isDuplicate,
      sampleName.intern,
      referenceContig,
      alignmentQuality,
      start,
      cigar,
      mdTagString,
      failedVendorQualityChecks,
      isPositiveStrand,
      isPaired
    )
  }

  /**
   * Converts the ascii-string encoded base qualities into an array of integers
   * quality scores in Phred-scale
   *
   * @param baseQualities Base qualities of a read (ascii-encoded)
   * @param length Length of the read sequence
   * @return  Base qualities in Phred scale
   */
  def baseQualityStringToArray(baseQualities: String, length: Int): Array[Byte] = {

    // If no base qualities are set, we set them all to 0.
    if (baseQualities.isEmpty)
      (0 until length).map(_ => 0.toByte).toArray
    else
      baseQualities.map(q => (q - 33).toByte).toArray

  }

  /**
   * Convert a SAM tools record into a Read.
   *
   * @param record
   * @return
   */
  def fromSAMRecord(record: SAMRecord,
                    token: Int,
                    requireMDTagsOnMappedReads: Boolean,
                    referenceGenome: Option[ReferenceGenome]): Read = {

    val isMapped = (
      // NOTE(ryan): this flag should maybe be the main determinant of the mapped-ness of this SAM record. SAM spec
      // (http://samtools.github.io/hts-specs/SAMv1.pdf) says: "Bit 0x4 is the only reliable place to tell whether the
      // read is unmapped."
      !record.getReadUnmappedFlag &&
      record.getReferenceName != null &&
      record.getReferenceIndex >= SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX &&
      record.getAlignmentStart >= 0 &&
      record.getUnclippedStart >= 0)

    val sampleName = (if (record.getReadGroup != null && record.getReadGroup.getSample != null) {
      record.getReadGroup.getSample
    } else {
      "default"
    }).intern

    val read = if (isMapped) {
      val mdTagString =
        referenceGenome.map(
          _.buildMdTag(record.getReadString, record.getReferenceName, record.getAlignmentStart - 1, record.getCigar)
        ).orElse(Option(record.getStringAttribute("MD")))

      if (!mdTagString.isDefined && requireMDTagsOnMappedReads) {
        throw MissingMDTagException(record)
      }

      val result = MappedRead(
        token,
        record.getReadString.getBytes,
        record.getBaseQualities,
        record.getDuplicateReadFlag,
        sampleName.intern,
        record.getReferenceName.intern,
        record.getMappingQuality,
        record.getAlignmentStart - 1,
        cigar = record.getCigar,
        mdTagString = mdTagString,
        failedVendorQualityChecks = record.getReadFailsVendorQualityCheckFlag,
        isPositiveStrand = !record.getReadNegativeStrandFlag,
        isPaired = record.getReadPairedFlag
      )

      // We subtract 1 from start, since samtools is 1-based and we're 0-based.
      if (result.unclippedStart != record.getUnclippedStart - 1)
        log.warn("Computed read 'unclippedStart' %d != samtools read end %d.".format(
          result.unclippedStart, record.getUnclippedStart - 1))
      result
    } else {
      UnmappedRead(
        token,
        record.getReadString.getBytes,
        record.getBaseQualities,
        record.getDuplicateReadFlag,
        sampleName,
        record.getReadFailsVendorQualityCheckFlag,
        record.getReadPairedFlag
      )
    }
    if (record.getReadPairedFlag) {
      val mateAlignment = MateAlignmentProperties(record)
      PairedRead(read, isFirstInPair = record.getFirstOfPairFlag, mateAlignment)
    } else {
      read
    }
  }

  case class ReadLoadingConfig(bamReaderAPI: BamReaderAPI = BamReaderAPI.Best) {}
  object ReadLoadingConfig {
    val default = ReadLoadingConfig()
  }

  object BamReaderAPI extends Enumeration {
    type BamReaderAPI = Value
    val Best, Samtools, HadoopBAM = Value
  }

  /**
   * Given a filename and a spark context, return a pair (RDD, SequenceDictionary), where the first element is an RDD
   * of Reads, and the second element is the Sequence Dictionary giving info (e.g. length) about the contigs in the BAM.
   *
   * @param filename name of file containing reads
   * @param sc spark context
   * @param token value to set the "token" field to in all the reads (default 0)
   * @param filters filters to apply
   * @return
   */

  def loadReadRDDAndSequenceDictionary(filename: String,
                                       sc: SparkContext,
                                       token: Int,
                                       filters: InputFilters,
                                       requireMDTagsOnMappedReads: Boolean,
                                       referenceGenome: Option[ReferenceGenome] = None,
                                       config: ReadLoadingConfig = ReadLoadingConfig.default): (RDD[Read], SequenceDictionary) = {
    if (filename.endsWith(".bam") || filename.endsWith(".sam")) {
      loadReadRDDAndSequenceDictionaryFromBAM(
        filename,
        sc,
        token,
        filters,
        requireMDTagsOnMappedReads,
        referenceGenome,
        config
      )
    } else {
      loadReadRDDAndSequenceDictionaryFromADAM(
        filename,
        sc,
        token,
        filters,
        referenceGenome,
        config
      )
    }
  }

  /** Returns an RDD of Reads and SequenceDictionary from reads in BAM format **/
  def loadReadRDDAndSequenceDictionaryFromBAM(
    filename: String,
    sc: SparkContext,
    token: Int = 0,
    filters: InputFilters = InputFilters.empty,
    requireMDTagsOnMappedReads: Boolean,
    referenceGenome: Option[ReferenceGenome],
    config: ReadLoadingConfig = ReadLoadingConfig.default): (RDD[Read], SequenceDictionary) = {

    val path = new Path(filename)
    val useSamtools =
      config.bamReaderAPI == BamReaderAPI.Samtools ||
        (config.bamReaderAPI == BamReaderAPI.Best &&
          path.getFileSystem(sc.hadoopConfiguration).getScheme == "file")

    if (useSamtools) {
      // Load with samtools
      var requiresFilteringByLocus = filters.overlapsLoci.nonEmpty

      val factory = SamReaderFactory.makeDefault
      val reader = factory.open(new File(path.toUri.getPath))
      val samSequenceDictionary = reader.getFileHeader.getSequenceDictionary
      val sequenceDictionary = SequenceDictionary.fromSAMSequenceDictionary(samSequenceDictionary)
      val loci = filters.overlapsLoci.map(_.result(contigLengths(sequenceDictionary)))
      val recordIterator: SAMRecordIterator = if (filters.overlapsLoci.nonEmpty && reader.hasIndex) {
        logInfo("Using samtools with BAM index to read: %s".format(filename))
        requiresFilteringByLocus = false
        val queryIntervals = loci.get.contigs.flatMap(contig => {
          val contigIndex = samSequenceDictionary.getSequenceIndex(contig)
          loci.get.onContig(contig).ranges().map(range =>
            new QueryInterval(contigIndex,
              range.start.toInt + 1, // convert 0-indexed inclusive to 1-indexed inclusive
              range.end.toInt)) // "convert" 0-indexed exclusive to 1-indexed inclusive, which is a no-op)
        })
        val optimizedQueryIntervals = QueryInterval.optimizeIntervals(queryIntervals.toArray)
        reader.query(optimizedQueryIntervals, false)
      } else {
        logInfo("Using samtools without BAM index to read: %s".format(filename))
        reader.iterator
      }
      val reads = JavaConversions.asScalaIterator(recordIterator).flatMap(record => {
        // Optimization: some of the filters are easy to run on the raw SamRecord, so we avoid making a Read.
        if ((requiresFilteringByLocus && (
          record.getReadUnmappedFlag ||
          (!loci.get.onContig(record.getContig).contains(record.getAlignmentStart) &&
            !loci.get.onContig(record.getContig).intersects(record.getUnclippedStart - 1, record.getUnclippedEnd)))) ||
            (filters.nonDuplicate && record.getDuplicateReadFlag) ||
            (filters.passedVendorQualityChecks && record.getReadFailsVendorQualityCheckFlag) ||
            (filters.isPaired && !record.getReadPairedFlag)) {
          None
        } else {
          val read = fromSAMRecord(record, token, requireMDTagsOnMappedReads, referenceGenome)
          if (filters.hasMdTag && !read.hasMdTag) {
            None
          } else {
            Some(read)
          }
        }
      })
      (sc.parallelize(reads.toSeq), sequenceDictionary)
    } else {
      // Load with hadoop bam
      logInfo("Using hadoop bam to read: %s".format(filename))
      val samHeader = SAMHeaderReader.readSAMHeaderFrom(path, sc.hadoopConfiguration)
      val sequenceDictionary = SequenceDictionary.fromSAMHeader(samHeader)

      val samRecords: RDD[(LongWritable, SAMRecordWritable)] =
        sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, AnySAMInputFormat](filename)
      val allReads: RDD[Read] =
        samRecords.map({
          case (k, v) => fromSAMRecord(
            v.get,
            token,
            requireMDTagsOnMappedReads,
            referenceGenome)
        })
      val reads = InputFilters.filterRDD(filters, allReads, sequenceDictionary)
      (reads, sequenceDictionary)
    }
  }

  /** Returns an RDD of Reads and SequenceDictionary from reads in ADAM format **/
  def loadReadRDDAndSequenceDictionaryFromADAM(filename: String,
                                               sc: SparkContext,
                                               token: Int = 0,
                                               filters: InputFilters = InputFilters.empty,
                                               referenceGenome: Option[ReferenceGenome] = None,
                                               config: ReadLoadingConfig = ReadLoadingConfig.default): (RDD[Read], SequenceDictionary) = {

    val adamContext = new ADAMContext(sc)

    // Build a projection that will only load the fields we will need to populate a Read
    val ADAMSpecificProjection = Projection(
      AlignmentRecordField.recordGroupSample,

      AlignmentRecordField.sequence,
      AlignmentRecordField.qual,

      // Alignment properties
      AlignmentRecordField.readMapped,
      AlignmentRecordField.contig,
      AlignmentRecordField.start,
      AlignmentRecordField.readNegativeStrand,
      AlignmentRecordField.cigar,
      AlignmentRecordField.mapq,
      AlignmentRecordField.mismatchingPositions,

      // Filter flags
      AlignmentRecordField.duplicateRead,
      AlignmentRecordField.failedVendorQualityChecks,

      // Mate fields
      AlignmentRecordField.readPaired,
      AlignmentRecordField.mateMapped,
      AlignmentRecordField.firstOfPair,
      AlignmentRecordField.mateContig,
      AlignmentRecordField.mateAlignmentStart,
      AlignmentRecordField.mateNegativeStrand,
      AlignmentRecordField.recordGroupPredictedMedianInsertSize
    )

    val adamRecords: RDD[AlignmentRecord] = adamContext.loadParquet(filename, projection = Some(ADAMSpecificProjection))
    val sequenceDictionary = new ADAMSpecificRecordSequenceDictionaryRDDAggregator(adamRecords).adamGetSequenceDictionary()

    val allReads: RDD[Read] = adamRecords.map(fromADAMRecord(_, token, referenceGenome))
    val reads = InputFilters.filterRDD(filters, allReads, sequenceDictionary)
    (reads, sequenceDictionary)
  }

  /**
   *
   * Builds a Guacamole Read from and ADAM Alignment Record
   *
   * @param alignmentRecord ADAM Alignment Record (an aligned or unaligned read)
   * @return Mapped or Unmapped Read
   */
  def fromADAMRecord(alignmentRecord: AlignmentRecord, token: Int, referenceGenome: Option[ReferenceGenome]): Read = {

    val sequence = Bases.stringToBases(alignmentRecord.getSequence.toString)
    val baseQualities = baseQualityStringToArray(alignmentRecord.getQual.toString, sequence.length)

    val referenceContig = alignmentRecord.getContig.getContigName.intern
    val cigar = TextCigarCodec.decode(alignmentRecord.getCigar)
    val mdTagString =
      referenceGenome.map(
        _.buildMdTag(alignmentRecord.getSequence, referenceContig, alignmentRecord.getStart.toInt - 1, cigar)
      ).orElse(Option(alignmentRecord.getMismatchingPositions))

    val read = if (alignmentRecord.getReadMapped) {
      MappedRead(
        token = token,
        sequence = sequence,
        baseQualities = baseQualities,
        isDuplicate = alignmentRecord.getDuplicateRead,
        sampleName = alignmentRecord.getRecordGroupSample.toString.intern(),
        referenceContig = referenceContig,
        alignmentQuality = alignmentRecord.getMapq,
        start = alignmentRecord.getStart,
        cigar = cigar,
        mdTagString = mdTagString,
        failedVendorQualityChecks = alignmentRecord.getFailedVendorQualityChecks,
        isPositiveStrand = !alignmentRecord.getReadNegativeStrand,
        alignmentRecord.getReadPaired
      )
    } else {
      UnmappedRead(
        token = token,
        sequence = sequence,
        baseQualities = baseQualities,
        isDuplicate = alignmentRecord.getDuplicateRead,
        sampleName = alignmentRecord.getRecordGroupSample.toString.intern(),
        failedVendorQualityChecks = alignmentRecord.getFailedVendorQualityChecks,
        alignmentRecord.getReadPaired
      )
    }

    if (alignmentRecord.getReadPaired) {
      val mateAlignment = if (alignmentRecord.getMateMapped) Some(
        MateAlignmentProperties(
          referenceContig = alignmentRecord.getMateContig.toString,
          start = alignmentRecord.getMateAlignmentStart,
          inferredInsertSize = None,
          isPositiveStrand = !alignmentRecord.getMateNegativeStrand
        )
      )
      else
        None
      PairedRead(read, isFirstInPair = alignmentRecord.getReadNum == 1, mateAlignment)
    } else {
      read
    }
  }

  /** Is the given samtools CigarElement a (hard/soft) clip? */
  def cigarElementIsClipped(element: CigarElement): Boolean = {
    element.getOperator == CigarOperator.SOFT_CLIP || element.getOperator == CigarOperator.HARD_CLIP
  }

  def contigLengths(sequenceDictionary: SequenceDictionary): Map[String, Long] = {
    val builder = Map.newBuilder[String, Long]
    sequenceDictionary.records.foreach(record => builder += ((record.name.toString, record.length)))
    builder.result
  }

}
