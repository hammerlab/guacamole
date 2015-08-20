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
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.projections.{ AlignmentRecordField, Projection }
import org.bdgenomics.adam.rdd.{ ADAMContext, ADAMSpecificRecordSequenceDictionaryRDDAggregator }
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.reference.ReferenceGenome
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{ AnySAMInputFormat, SAMRecordWritable }

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

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

  /** The sample (e.g. "tumor" or "patient3636") name. */
  def sampleName: String

  /** Whether the read failed predefined vendor checks for quality */
  def failedVendorQualityChecks: Boolean

  /** Whether the read was on the positive or forward strand */
  def isPositiveStrand: Boolean

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
  case class InputFilters(
    mapped: Boolean = false,
    nonDuplicate: Boolean = false,
    passedVendorQualityChecks: Boolean = false,
    isPaired: Boolean = false,
    hasMdTag: Boolean = false) {}
  object InputFilters {
    val empty = InputFilters()
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
   *
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

    if (isMapped) {
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
        !record.getReadNegativeStrandFlag,
        record.getReadPairedFlag
      )
    }
  }

  /**
   * Given a local path to a BAM/SAM file, return a pair: (Array, SequenceDictionary), where the first element is an
   * array of the reads, and the second is a Sequence Dictionary giving info (e.g. length) about the contigs in the BAM.
   */
  def loadReadArrayAndSequenceDictionaryFromBAM(
    filename: String,
    token: Int,
    filters: InputFilters,
    referenceGenome: Option[ReferenceGenome]): (ArrayBuffer[Read], SequenceDictionary) = {
    val reader = SamReaderFactory.make.open(new java.io.File(filename))
    val sequenceDictionary = SequenceDictionary.fromSAMReader(reader)
    val result = new ArrayBuffer[Read]

    JavaConversions.asScalaIterator(reader.iterator).foreach(item => {
      if (!filters.nonDuplicate || !item.getDuplicateReadFlag) {
        val read = fromSAMRecord(
          item,
          token,
          requireMDTagsOnMappedReads = false,
          referenceGenome
        )

        if ((!filters.mapped || read.isMapped) &&
          (!filters.passedVendorQualityChecks || !read.failedVendorQualityChecks) &&
          (!filters.isPaired || read.isPaired)) {
          result += read
        }
      }
    })
    (result, sequenceDictionary)
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
                                       referenceGenome: Option[ReferenceGenome] = None): (RDD[Read], SequenceDictionary) = {
    var (reads, sequenceDictionary) = if (filename.endsWith(".bam") || filename.endsWith(".sam")) {
      loadReadRDDAndSequenceDictionaryFromBAM(
        filename,
        sc,
        token,
        requireMDTagsOnMappedReads,
        referenceGenome
      )
    } else {
      loadReadRDDAndSequenceDictionaryFromADAM(
        filename,
        sc,
        token,
        referenceGenome
      )
    }
    if (filters.mapped) reads = reads.filter(_.isMapped)
    if (filters.nonDuplicate) reads = reads.filter(!_.isDuplicate)
    if (filters.passedVendorQualityChecks) reads = reads.filter(!_.failedVendorQualityChecks)
    if (filters.isPaired) reads = reads.filter(_.isPaired)
    if (filters.hasMdTag) reads = reads.filter(_.hasMdTag)
    (reads, sequenceDictionary)

  }

  /** Returns an RDD of Reads and SequenceDictionary from reads in BAM format **/
  def loadReadRDDAndSequenceDictionaryFromBAM(
    filename: String,
    sc: SparkContext,
    token: Int,
    requireMDTagsOnMappedReads: Boolean,
    referenceGenome: Option[ReferenceGenome]): (RDD[Read], SequenceDictionary) = {

    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filename), sc.hadoopConfiguration)
    val sequenceDictionary = SequenceDictionary.fromSAMHeader(samHeader)

    val samRecords: RDD[(LongWritable, SAMRecordWritable)] =
      sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, AnySAMInputFormat](filename)
    val reads: RDD[Read] =
      samRecords.map({
        case (k, v) =>
          if (v.get.getReadPairedFlag)
            PairedRead(
              v.get,
              token,
              requireMDTagsOnMappedReads,
              referenceGenome
            )
          else
            fromSAMRecord(
              v.get,
              token,
              requireMDTagsOnMappedReads,
              referenceGenome
            )
      })
    (reads, sequenceDictionary)
  }

  /** Returns an RDD of Reads and SequenceDictionary from reads in ADAM format **/
  def loadReadRDDAndSequenceDictionaryFromADAM(filename: String,
                                               sc: SparkContext,
                                               token: Int,
                                               referenceGenome: Option[ReferenceGenome] = None): (RDD[Read], SequenceDictionary) = {

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

    val reads: RDD[Read] = adamRecords.map(fromADAMRecord(_, token, referenceGenome))

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
        isPositiveStrand = !alignmentRecord.getReadNegativeStrand,
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
      PairedRead(read, isFirstInPair = alignmentRecord.getFirstOfPair, mateAlignment)
    } else {
      read
    }
  }

  /** Is the given samtools CigarElement a (hard/soft) clip? */
  def cigarElementIsClipped(element: CigarElement): Boolean = {
    element.getOperator == CigarOperator.SOFT_CLIP || element.getOperator == CigarOperator.HARD_CLIP
  }
}
