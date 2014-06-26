package org.bdgenomics.guacamole

import net.sf.samtools._
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.LongWritable
import fi.tkk.ics.hadoop.bam.{ AnySAMInputFormat, SAMRecordWritable }
import scala.collection.mutable.ArrayBuffer
import org.bdgenomics.adam.models.SequenceDictionary
import fi.tkk.ics.hadoop.bam.util.SAMHeaderReader
import org.apache.hadoop.fs.Path
import scala.collection.JavaConversions
import scala.Some
import org.bdgenomics.adam.util.MdTag
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }

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
  val sequence: Array[Byte]

  /** The base qualities, phred scaled.  These are numbers, and are NOT character encoded. */
  val baseQualities: Array[Byte]

  /** Is this read a duplicate of another? */
  val isDuplicate: Boolean

  /** Is this read mapped? */
  val isMapped: Boolean

  /** The sample (e.g. "tumor" or "patient3636") name. */
  val sampleName: String

  /** If isMapped=true, will return the corresponding MappedRead. Otherwise, throws an error. */
  def getMappedRead(): MappedRead

  /** Whether the read failed predefined vendor checks for quality */
  val failedVendorQualityChecks: Boolean

  /** Whether the read was on the positive or forward strand */
  val isPositiveStrand: Boolean

  /** Whether read is from a paired-end library */
  val isPaired: Boolean

  /** Distance between the first base and the last base in the paired reads */
  val inferredInsertSize: Option[Int]
}

/**
 * An unmapped read. See the [[Read]] trait for field descriptions.
 *
 */
case class UnmappedRead(
    token: Int,
    sequence: Array[Byte],
    baseQualities: Array[Byte],
    isDuplicate: Boolean,
    sampleName: String,
    failedVendorQualityChecks: Boolean,
    isPositiveStrand: Boolean,
    isPaired: Boolean,
    inferredInsertSize: Option[Int]) extends Read {

  assert(baseQualities.length == sequence.length)

  val isMapped = false
  override def getMappedRead(): MappedRead = throw new AssertionError("Not a mapped read.")
}

/**
 * A mapped read. See the [[Read]] trait for some of the field descriptions.
 *
 * @param referenceContig the contig name (e.g. "chr12") that this read was mapped to.
 * @param alignmentQuality the mapping quality, phred scaled.
 * @param start the reference locus that the first base in this read aligns to.
 * @param cigar parsed samtools CIGAR object.
 * @param mdTag parsed ADAM MdTag object.
 * @param failedVendorQualityChecks read failed sequencing quality checks
 * @param isPositiveStrand read is on the forward or positive strand
 */
case class MappedRead(
    token: Int,
    sequence: Array[Byte],
    baseQualities: Array[Byte],
    isDuplicate: Boolean,
    sampleName: String,
    referenceContig: String,
    alignmentQuality: Int,
    start: Long,
    cigar: Cigar,
    mdTag: Option[MdTag],
    failedVendorQualityChecks: Boolean,
    isPositiveStrand: Boolean,
    isPaired: Boolean,
    inferredInsertSize: Option[Int]) extends Read {

  assert(baseQualities.length == sequence.length,
    "Base qualities have length %d but sequence has length %d".format(baseQualities.length, sequence.length))

  override val isMapped = true
  override def getMappedRead(): MappedRead = this

  /** Individual components of the CIGAR string (e.g. "10M"), parsed, and as a Scala buffer. */
  val cigarElements = JavaConversions.asScalaBuffer(cigar.getCigarElements)

  /**
   * The end of the alignment, exclusive. This is the first reference locus AFTER the locus corresponding to the last
   * base in this read.
   */
  val end: Long = start + cigar.getPaddedReferenceLength

  /**
   * A read can be "clipped", meaning that some prefix or suffix of it did not align. This is the start of the whole
   * read's alignment, including any initial clipped bases.
   */
  val unclippedStart = cigarElements.takeWhile(Read.cigarElementIsClipped).foldLeft(start)({
    (pos, element) => pos - element.getLength
  })

  /**
   * The end of the read's alignment, including any final clipped bases, exclusive.
   */
  val unclippedEnd = cigarElements.reverse.takeWhile(Read.cigarElementIsClipped).foldLeft(end)({
    (pos, element) => pos + element.getLength
  })

  /**
   * Does this read overlap any of the given loci, with halfWindowSize padding?
   */
  def overlapsLociSet(loci: LociSet, halfWindowSize: Long = 0): Boolean = {
    loci.onContig(referenceContig).intersects(math.max(0, start - halfWindowSize), end + halfWindowSize)
  }

  /**
   * Does the read overlap the given locus, with halfWindowSize padding?
   */
  def overlapsLocus(locus: Long, halfWindowSize: Long = 0): Boolean = {
    start - halfWindowSize <= locus && end + halfWindowSize > locus
  }
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
   * @param hasMdTag include only reads that are both mapped and have md tags defined.
   */
  case class InputFilters(
    mapped: Boolean = false,
    nonDuplicate: Boolean = false,
    passedVendorQualityChecks: Boolean = false,
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
    mdTagString: String = "",
    failedVendorQualityChecks: Boolean = false,
    isPositiveStrand: Boolean = true,
    isPaired: Boolean = false,
    inferredInsertSize: Option[Int] = None): Read = {

    val sequenceArray = sequence.map(_.toByte).toArray
    val qualityScoresArray = {
      // If no base qualities are set, we set them all to 0.
      if (baseQualities.isEmpty)
        sequenceArray.map(_ => 0.toByte).toSeq.toArray
      else
        baseQualities.map(q => (q - 33).toByte).toArray
    }

    if (referenceContig.isEmpty) {
      UnmappedRead(token, sequenceArray, qualityScoresArray, isDuplicate, sampleName.intern, failedVendorQualityChecks, isPositiveStrand, isPaired, inferredInsertSize)
    } else {
      val cigar = TextCigarCodec.getSingleton.decode(cigarString)
      val mdTag = if (mdTagString.isEmpty) None else Some(MdTag(mdTagString, start))
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
        mdTag,
        failedVendorQualityChecks,
        isPositiveStrand,
        isPaired,
        inferredInsertSize)
    }
  }

  /**
   * Convert a SAM tools record into a Read.
   *
   * @param record
   * @return
   */
  def fromSAMRecord(record: SAMRecord, token: Int = 0): Read = {
    val isMapped = (
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

    if (isMapped) {
      val mdTagString = record.getStringAttribute("MD")
      val mdTag = if (mdTagString == null || mdTagString.isEmpty)
        None
      else
        Some(MdTag(mdTagString, record.getAlignmentStart - 1))
      val result = MappedRead(
        token,
        record.getReadString.getBytes,
        record.getBaseQualities,
        record.getDuplicateReadFlag,
        sampleName,
        record.getReferenceName.intern,
        record.getMappingQuality,
        record.getAlignmentStart - 1,
        cigar = record.getCigar,
        mdTag = mdTag,
        failedVendorQualityChecks = record.getReadFailsVendorQualityCheckFlag,
        isPositiveStrand = !record.getReadNegativeStrandFlag,
        isPaired = record.getReadPairedFlag,
        inferredInsertSize = Some(record.getInferredInsertSize))

      // We subtract 1 from start, since samtools is 1-based and we're 0-based.
      if (result.unclippedStart != record.getUnclippedStart - 1)
        log.warn("Computed read 'unclippedStart' %d != samtools read end %d.".format(
          result.unclippedStart, record.getUnclippedStart - 1))
      result
    } else {
      val result = UnmappedRead(
        token,
        record.getReadString.getBytes,
        record.getBaseQualities,
        record.getDuplicateReadFlag,
        sampleName,
        record.getReadFailsVendorQualityCheckFlag,
        !record.getReadNegativeStrandFlag,
        record.getReadPairedFlag,
        Some(record.getInferredInsertSize))
      result
    }
  }

  /**
   * Given a local path to a BAM/SAM file, return a pair: (Array, SequenceDictionary), where the first element is an
   * array of the reads, and the second is a Sequence Dictionary giving info (e.g. length) about the contigs in the BAM.
   */
  def loadReadArrayAndSequenceDictionaryFromBAM(
    filename: String,
    token: Int = 0,
    filters: InputFilters): (ArrayBuffer[Read], SequenceDictionary) = {
    val reader = new SAMFileReader(new java.io.File(filename))
    val sequenceDictionary = SequenceDictionary.fromSAMReader(reader)
    val result = new ArrayBuffer[Read]

    JavaConversions.asScalaIterator(reader.iterator).foreach(item => {
      if (!filters.nonDuplicate || !item.getDuplicateReadFlag) {
        val read = fromSAMRecord(item)
        if ((!filters.mapped || read.isMapped) &&
          (!filters.passedVendorQualityChecks || !read.failedVendorQualityChecks) &&
          (!filters.hasMdTag || read.isMapped && read.getMappedRead.mdTag.isDefined))
          result += read
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
    token: Int = 0,
    filters: InputFilters): (RDD[Read], SequenceDictionary) = {

    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filename), sc.hadoopConfiguration)
    val sequenceDictionary = SequenceDictionary.fromSAMHeader(samHeader)

    val samRecords: RDD[(LongWritable, SAMRecordWritable)] =
      sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, AnySAMInputFormat](filename)
    var reads: RDD[Read] = samRecords.map({ case (k, v) => fromSAMRecord(v.get) })
    if (filters.mapped) reads = reads.filter(_.isMapped)
    if (filters.nonDuplicate) reads = reads.filter(read => !read.isDuplicate)
    if (filters.passedVendorQualityChecks) reads = reads.filter(read => !read.failedVendorQualityChecks)
    if (filters.hasMdTag) reads = reads.filter(read => read.isMapped && read.getMappedRead.mdTag.isDefined)
    (reads, sequenceDictionary)
  }

  /** Is the given samtools CigarElement a (hard/soft) clip? */
  def cigarElementIsClipped(element: CigarElement): Boolean = {
    element.getOperator == CigarOperator.SOFT_CLIP || element.getOperator == CigarOperator.HARD_CLIP
  }
}

// Serialization: MappedRead
class MappedReadSerializer extends Serializer[MappedRead] {
  def write(kryo: Kryo, output: Output, obj: MappedRead) = {
    output.writeInt(obj.token)
    assert(obj.sequence.length == obj.baseQualities.length)
    output.writeInt(obj.sequence.length, true)
    output.writeBytes(obj.sequence)
    output.writeBytes(obj.baseQualities)
    output.writeBoolean(obj.isDuplicate)
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeInt(obj.alignmentQuality, true)
    output.writeLong(obj.start, true)
    output.writeString(obj.cigar.toString)
    output.writeString(obj.mdTag.map(_.toString).getOrElse(""))
    output.writeBoolean(obj.failedVendorQualityChecks)
    output.writeBoolean(obj.isPositiveStrand)
    output.writeBoolean(obj.isPaired)
    obj.inferredInsertSize match {
      case None =>
        output.writeBoolean(false)
      case Some(insertSize) =>
        output.writeBoolean(true)
        output.writeInt(insertSize)
    }
  }

  def read(kryo: Kryo, input: Input, klass: Class[MappedRead]): MappedRead = {
    val token = input.readInt()
    val count: Int = input.readInt(true)
    val sequenceArray: Array[Byte] = input.readBytes(count)
    val qualityScoresArray = input.readBytes(count)
    val isDuplicate = input.readBoolean()
    val sampleName = input.readString().intern()
    val referenceContig = input.readString().intern()
    val alignmentQuality = input.readInt(true)
    val start = input.readLong(true)
    val cigarString = input.readString()
    val mdTagString = input.readString()
    val failedVendorQualityChecks = input.readBoolean()
    val isPositiveStrand = input.readBoolean()
    val isPairedRead = input.readBoolean()
    val hasInferredInsertSize = input.readBoolean()
    val inferredInsertSize = if (hasInferredInsertSize) Some(input.readInt()) else None

    val cigar = TextCigarCodec.getSingleton.decode(cigarString)
    val mdTag = if (mdTagString.isEmpty) None else Some(MdTag(mdTagString, start))
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
      mdTag,
      failedVendorQualityChecks,
      isPositiveStrand,
      isPairedRead,
      inferredInsertSize)
  }
}

// Serialization: UnmappedRead
class UnmappedReadSerializer extends Serializer[UnmappedRead] {
  def write(kryo: Kryo, output: Output, obj: UnmappedRead) = {
    output.writeInt(obj.token)
    assert(obj.sequence.length == obj.baseQualities.length)
    output.writeInt(obj.sequence.length, true)
    output.writeBytes(obj.sequence)
    output.writeBytes(obj.baseQualities)
    output.writeBoolean(obj.isDuplicate)
    output.writeString(obj.sampleName)
    output.writeBoolean(obj.failedVendorQualityChecks)
    output.writeBoolean(obj.isPositiveStrand)
    output.writeBoolean(obj.isPaired)
    obj.inferredInsertSize match {
      case None =>
        output.writeBoolean(false)
      case Some(insertSize) =>
        output.writeBoolean(true)
        output.writeInt(insertSize)
    }
  }

  def read(kryo: Kryo, input: Input, klass: Class[UnmappedRead]): UnmappedRead = {
    val token = input.readInt()
    val count: Int = input.readInt(true)
    val sequenceArray: Array[Byte] = input.readBytes(count)
    val qualityScoresArray = input.readBytes(count)
    val isDuplicate = input.readBoolean()
    val sampleName = input.readString().intern()
    val failedVendorQualityChecks = input.readBoolean()
    val isPositiveStrand = input.readBoolean()
    val isPairedRead = input.readBoolean()
    val hasInferredInsertSize = input.readBoolean()
    val inferredInsertSize = if (hasInferredInsertSize) Some(input.readInt()) else None

    UnmappedRead(
      token,
      sequenceArray,
      qualityScoresArray,
      isDuplicate,
      sampleName.intern,
      failedVendorQualityChecks,
      isPositiveStrand,
      isPairedRead,
      inferredInsertSize)
  }
}