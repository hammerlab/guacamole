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

import htsjdk.samtools._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.{ADAMContext, ADAMSpecificRecordSequenceDictionaryRDDAggregator}
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.guacamole.loci.LociSet
import org.hammerlab.guacamole.{Bases, Common}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, BAMInputFormat, SAMRecordWritable}

import scala.collection.JavaConversions._

/**
 * The fields in the Read trait are common to any read, whether mapped (aligned) or not.
 */
trait Read {

  /* The template name. A read, its mate, and any alternate alignments have the same name. */
  def name: String

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

}

object Read extends Logging {
  /**
   * Filtering reads while they are loaded can be an important optimization.
   *
   * These fields are commonly used filters. For boolean fields, setting a field to true will result in filtering on
   * that field. The result is the intersection of the filters (i.e. reads must satisfy ALL filters).
   *
   * @param overlapsLoci if not None, include only mapped reads that overlap the given loci
   * @param nonDuplicate include only reads that do not have the duplicate bit set
   * @param passedVendorQualityChecks include only reads that do not have the failedVendorQualityChecks bit set
   * @param isPaired include only reads are paired-end reads
   */
  case class InputFilters(
    overlapsLoci: Option[LociSet.Builder],
    nonDuplicate: Boolean,
    passedVendorQualityChecks: Boolean,
    isPaired: Boolean) {}
  object InputFilters {
    val empty = InputFilters()

    /**
     * See InputFilters for full documentation.
     *
     * @param mapped include only mapped reads. Convenience argument that is equivalent to specifying all sites in
     *               overlapsLoci.
     */
    def apply(mapped: Boolean = false,
              overlapsLoci: Option[LociSet.Builder] = None,
              nonDuplicate: Boolean = false,
              passedVendorQualityChecks: Boolean = false,
              isPaired: Boolean = false): InputFilters = {
      new InputFilters(
        overlapsLoci = if (overlapsLoci.isEmpty && mapped) Some(LociSet.newBuilder.putAllContigs) else overlapsLoci,
        nonDuplicate = nonDuplicate,
        passedVendorQualityChecks = passedVendorQualityChecks,
        isPaired = isPaired)
    }

    /**
     * Apply filters to an RDD of reads.
     *
     * @param filters
     * @param reads
     * @param sequenceDictionary
     * @return filtered RDD
     */
    def filterRDD(filters: InputFilters,
                  reads: RDD[Read],
                  sequenceDictionary: SequenceDictionary): RDD[Read] = {
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
      result
    }
  }

  /**
   * Convenience function (intended for test cases), to construct a Read from unparsed values.
   */
  def apply(
    sequence: String,
    name: String,
    baseQualities: String = "",
    isDuplicate: Boolean = false,
    sampleName: String = "",
    referenceContig: String = "",
    alignmentQuality: Int = -1,
    start: Long = -1L,
    cigarString: String = "",
    failedVendorQualityChecks: Boolean = false,
    isPositiveStrand: Boolean = true,
    isPaired: Boolean = true) = {

    val sequenceArray = sequence.map(_.toByte).toArray
    val qualityScoresArray = baseQualityStringToArray(baseQualities, sequenceArray.length)

    val cigar = TextCigarCodec.decode(cigarString)
    MappedRead(
      name,
      sequenceArray,
      qualityScoresArray,
      isDuplicate,
      sampleName.intern,
      referenceContig,
      alignmentQuality,
      start,
      cigar,
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
  def fromSAMRecord(record: SAMRecord): Read = {

    val isMapped =
      !record.getReadUnmappedFlag &&
      record.getReferenceName != null &&
      record.getReferenceIndex >= SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX &&
      record.getAlignmentStart >= 0 &&
      record.getUnclippedStart >= 0

    val sampleName = (if (record.getReadGroup != null && record.getReadGroup.getSample != null) {
      record.getReadGroup.getSample
    } else {
      "default"
    }).intern

    val read = if (isMapped) {
      val result = MappedRead(
        record.getReadName,
        record.getReadString.getBytes,
        record.getBaseQualities,
        record.getDuplicateReadFlag,
        sampleName.intern,
        record.getReferenceName.intern,
        record.getMappingQuality,
        record.getAlignmentStart - 1,
        cigar = record.getCigar,
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
        record.getReadName,
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

  /**
   * Given a filename and a spark context, return a pair (RDD, SequenceDictionary), where the first element is an RDD
   * of Reads, and the second element is the Sequence Dictionary giving info (e.g. length) about the contigs in the BAM.
   *
   * @param filename name of file containing reads
   * @param sc spark context
   * @param filters filters to apply
   * @return
   */
  def loadReadRDDAndSequenceDictionary(filename: String,
                                       sc: SparkContext,
                                       filters: InputFilters): (RDD[Read], SequenceDictionary) = {
    if (filename.endsWith(".bam") || filename.endsWith(".sam")) {
      loadReadRDDAndSequenceDictionaryFromBAM(
        filename,
        sc,
        filters
      )
    } else {
      loadReadRDDAndSequenceDictionaryFromADAM(
        filename,
        sc,
        filters
      )
    }
  }

  /** Returns an RDD of Reads and SequenceDictionary from reads in BAM format **/
  def loadReadRDDAndSequenceDictionaryFromBAM(filename: String,
                                              sc: SparkContext,
                                              filters: InputFilters = InputFilters.empty): (RDD[Read], SequenceDictionary) = {

    val path = new Path(filename)

    val conf = sc.hadoopConfiguration
    val samHeader = SAMHeaderReader.readSAMHeaderFrom(path, conf)
    val sequenceDictionary = SequenceDictionary.fromSAMHeader(samHeader)

    if (filters.overlapsLoci.isEmpty || filename.endsWith(".bam")) {
      filters
        .overlapsLoci
        .map(_.result(contigLengths(sequenceDictionary)).toHtsJDKIntervals)
        .foreach(BAMInputFormat.setIntervals(conf, _))
    } else if (filename.endsWith(".sam")) {
      log.warn(s"Loading SAM file: $filename with intervals specified. This requires parsing the entire file.")
    } else {
      throw new IllegalArgumentException(s"File $filename is not a BAM or SAM file")
    }

    val allReads =
      sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, AnySAMInputFormat](filename)
        .map {
          case (k, v) => fromSAMRecord(v.get)
        }

    val reads: RDD[Read] = InputFilters.filterRDD(filters, allReads, sequenceDictionary)
    (reads, sequenceDictionary)
  }

  /** Returns an RDD of Reads and SequenceDictionary from reads in ADAM format **/
  def loadReadRDDAndSequenceDictionaryFromADAM(filename: String,
                                               sc: SparkContext,
                                               filters: InputFilters = InputFilters.empty): (RDD[Read], SequenceDictionary) = {

    Common.progress("Using ADAM to read: %s".format(filename))

    val adamContext = new ADAMContext(sc)

    val adamRecords: RDD[AlignmentRecord] = adamContext.loadAlignments(
      filename, projection = None, stringency = ValidationStringency.LENIENT).rdd
    val sequenceDictionary = new ADAMSpecificRecordSequenceDictionaryRDDAggregator(adamRecords).adamGetSequenceDictionary()

    val allReads: RDD[Read] = adamRecords.map(fromADAMRecord(_))
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
  def fromADAMRecord(alignmentRecord: AlignmentRecord): Read = {

    val sequence = Bases.stringToBases(alignmentRecord.getSequence)
    val baseQualities = baseQualityStringToArray(alignmentRecord.getQual, sequence.length)

    val referenceContig = alignmentRecord.getContig.getContigName.intern
    val cigar = TextCigarCodec.decode(alignmentRecord.getCigar)

    val read = if (alignmentRecord.getReadMapped) {
      MappedRead(
        name = alignmentRecord.getReadName,
        sequence = sequence,
        baseQualities = baseQualities,
        isDuplicate = alignmentRecord.getDuplicateRead,
        sampleName = alignmentRecord.getRecordGroupSample.intern(),
        referenceContig = referenceContig,
        alignmentQuality = alignmentRecord.getMapq,
        start = alignmentRecord.getStart,
        cigar = cigar,
        failedVendorQualityChecks = alignmentRecord.getFailedVendorQualityChecks,
        isPositiveStrand = !alignmentRecord.getReadNegativeStrand,
        alignmentRecord.getReadPaired
      )
    } else {
      UnmappedRead(
        name = alignmentRecord.getReadName,
        sequence = sequence,
        baseQualities = baseQualities,
        isDuplicate = alignmentRecord.getDuplicateRead,
        sampleName = alignmentRecord.getRecordGroupSample.intern(),
        failedVendorQualityChecks = alignmentRecord.getFailedVendorQualityChecks,
        alignmentRecord.getReadPaired
      )
    }

    if (alignmentRecord.getReadPaired) {
      val mateAlignment =
        if (alignmentRecord.getMateMapped) Some(
          MateAlignmentProperties(
            referenceContig = alignmentRecord.getMateContig.getContigName.intern(),
            start = alignmentRecord.getMateAlignmentStart,
            inferredInsertSize = if (alignmentRecord.getInferredInsertSize != 0 && alignmentRecord.getInferredInsertSize != null) Some(alignmentRecord.getInferredInsertSize.toInt) else None,
            isPositiveStrand = !alignmentRecord.getMateNegativeStrand
          )
        )
        else
          None
      PairedRead(read, isFirstInPair = alignmentRecord.getReadInFragment == 1, mateAlignment)
    } else {
      read
    }
  }

  /** Is the given samtools CigarElement a (hard/soft) clip? */
  def cigarElementIsClipped(element: CigarElement): Boolean = {
    element.getOperator == CigarOperator.SOFT_CLIP || element.getOperator == CigarOperator.HARD_CLIP
  }

  /** Extract the length of each contig from a sequence dictionary */
  def contigLengths(sequenceDictionary: SequenceDictionary): Map[String, Long] = {
    val builder = Map.newBuilder[String, Long]
    sequenceDictionary.records.foreach(record => builder += ((record.name.toString, record.length)))
    builder.result
  }

}
