package org.bdgenomics.guacamole.reads

import fi.tkk.ics.hadoop.bam.util.SAMHeaderReader
import fi.tkk.ics.hadoop.bam.{ AnySAMInputFormat, SAMRecordWritable }
import net.sf.samtools._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.guacamole.Bases

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
  val token: Int

  /** The nucleotide sequence. */
  val sequence: Seq[Byte]
  lazy val sequenceStr = Bases.basesToString(sequence)

  /** The base qualities, phred scaled.  These are numbers, and are NOT character encoded. */
  val baseQualities: Seq[Byte]

  /** Is this read a duplicate of another? */
  val isDuplicate: Boolean

  /** Is this read mapped? */
  final val isMapped: Boolean = getMappedReadOpt.isDefined

  /** The sample (e.g. "tumor" or "patient3636") name. */
  val sampleName: String

  /** Returns this Read as a MappedRead iff isMapped=true, otherwise None. */
  def getMappedReadOpt: Option[MappedRead] = None

  /** Whether the read failed predefined vendor checks for quality */
  val failedVendorQualityChecks: Boolean

  /** Whether the read was on the positive or forward strand */
  val isPositiveStrand: Boolean

  val matePropertiesOpt: Option[MateProperties]

  /** Whether read is from a paired-end library */
  val isPaired: Boolean = matePropertiesOpt.isDefined

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
    isPaired: Boolean = false) {}
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
    mdTagString: String,
    failedVendorQualityChecks: Boolean = false,
    isPositiveStrand: Boolean = true,
    matePropertiesOpt: Option[MateProperties] = None): Read = {

    val sequenceArray = sequence.map(_.toByte).toArray
    val qualityScoresArray = {
      // If no base qualities are set, we set them all to 0.
      if (baseQualities.isEmpty)
        sequenceArray.map(_ => 0.toByte).toSeq.toArray
      else
        baseQualities.map(q => (q - 33).toByte).toArray
    }

    if (referenceContig.isEmpty) {
      UnmappedRead(
        token,
        sequenceArray,
        qualityScoresArray,
        isDuplicate,
        sampleName.intern,
        failedVendorQualityChecks,
        isPositiveStrand,
        matePropertiesOpt = matePropertiesOpt
      )
    } else {
      val cigar = TextCigarCodec.getSingleton.decode(cigarString)
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
        matePropertiesOpt = matePropertiesOpt
      )
    }
  }

  /**
   * Convert a SAM tools record into a Read.
   *
   * @param record
   * @return
   */
  def fromSAMRecordOpt(record: SAMRecord, token: Int, requireMDTagsOnMappedReads: Boolean = false): Option[Read] = {
    val isMapped = (
      // NOTE(ryan): this flag should maybe be the main determinant of the mapped-ness of this SAM record. SAM spec
      // (http://samtools.github.io/hts-specs/SAMv1.pdf) says: "Bit 0x4 is the only reliable place to tell whether the
      // read is unmapped."
      !record.getReadUnmappedFlag &&
      record.getMappingQuality != SAMRecord.UNKNOWN_MAPPING_QUALITY &&
      record.getReferenceName != null &&
      record.getReferenceIndex >= SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX &&
      record.getAlignmentStart >= 0 &&
      record.getUnclippedStart >= 0)

    val sampleName = (if (record.getReadGroup != null && record.getReadGroup.getSample != null) {
      record.getReadGroup.getSample
    } else {
      "default"
    }).intern

    val matePropertiesOpt =
      if (record.getReadPairedFlag) {
        Some(
          MateProperties(
            isFirstInPair = record.getFirstOfPairFlag,
            inferredInsertSize = Some(record.getInferredInsertSize),
            isMateMapped = !record.getMateUnmappedFlag,
            mateReferenceContig = Some(record.getMateReferenceName),
            mateStart = Some(record.getMateAlignmentStart),
            isMatePositiveStrand = !record.getMateNegativeStrandFlag
          )
        )
      } else {
        None
      }

    if (isMapped) {
      Option(record.getStringAttribute("MD")) match {
        case Some(mdTagString) =>
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
            matePropertiesOpt = matePropertiesOpt
          )

          // We subtract 1 from start, since samtools is 1-based and we're 0-based.
          if (result.unclippedStart != record.getUnclippedStart - 1)
            log.warn("Computed read 'unclippedStart' %d != samtools read end %d.".format(
              result.unclippedStart, record.getUnclippedStart - 1))
          Some(result)
        case None =>
          if (requireMDTagsOnMappedReads) {
            throw MissingMDTagException(record)
          } else {
            None
          }
      }

    } else {
      val result = UnmappedRead(
        token,
        record.getReadString.getBytes,
        record.getBaseQualities,
        record.getDuplicateReadFlag,
        sampleName,
        record.getReadFailsVendorQualityCheckFlag,
        !record.getReadNegativeStrandFlag,
        matePropertiesOpt = matePropertiesOpt
      )
      Some(result)
    }
  }

  /**
   * Given a local path to a BAM/SAM file, return a pair: (Array, SequenceDictionary), where the first element is an
   * array of the reads, and the second is a Sequence Dictionary giving info (e.g. length) about the contigs in the BAM.
   */
  def loadReadArrayAndSequenceDictionaryFromBAM(
    filename: String,
    token: Int,
    filters: InputFilters): (ArrayBuffer[Read], SequenceDictionary) = {
    val reader = new SAMFileReader(new java.io.File(filename))
    val sequenceDictionary = SequenceDictionary.fromSAMReader(reader)
    val result = new ArrayBuffer[Read]

    JavaConversions.asScalaIterator(reader.iterator).foreach(item => {
      if (!filters.nonDuplicate || !item.getDuplicateReadFlag) {
        fromSAMRecordOpt(item, token).filter(read =>
          (!filters.mapped || read.isMapped) &&
            (!filters.passedVendorQualityChecks || !read.failedVendorQualityChecks) &&
            (!filters.isPaired || read.isPaired)
        ).map(result += _)
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
  def loadReadRDDAndSequenceDictionaryFromBAM(
    filename: String,
    sc: SparkContext,
    token: Int,
    filters: InputFilters): (RDD[Read], SequenceDictionary) = {

    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filename), sc.hadoopConfiguration)
    val sequenceDictionary = SequenceDictionary.fromSAMHeader(samHeader)

    val samRecords: RDD[(LongWritable, SAMRecordWritable)] =
      sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, AnySAMInputFormat](filename)
    var reads: RDD[Read] =
      samRecords.flatMap({
        case (k, v) => fromSAMRecordOpt(v.get, token)
      })
    if (filters.mapped) reads = reads.filter(_.isMapped)
    if (filters.nonDuplicate) reads = reads.filter(!_.isDuplicate)
    if (filters.passedVendorQualityChecks) reads = reads.filter(!_.failedVendorQualityChecks)
    (reads, sequenceDictionary)
  }

  /** Is the given samtools CigarElement a (hard/soft) clip? */
  def cigarElementIsClipped(element: CigarElement): Boolean = {
    element.getOperator == CigarOperator.SOFT_CLIP || element.getOperator == CigarOperator.HARD_CLIP
  }
}
