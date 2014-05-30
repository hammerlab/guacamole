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

/**
 * The fields in the Read trait are common to any read, whether mapped (aligned) or not.
 */
trait Read {
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
}

/**
 * An unmapped read. See the [[Read]] trait for field descriptions.
 *
 */
case class UnmappedRead(
    sequence: Array[Byte],
    baseQualities: Array[Byte],
    isDuplicate: Boolean,
    sampleName: String) extends Read {

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
 */
case class MappedRead(
    sequence: Array[Byte],
    baseQualities: Array[Byte],
    isDuplicate: Boolean,
    sampleName: String,
    referenceContig: String,
    alignmentQuality: Int,
    start: Long,
    cigar: Cigar,
    mdTag: Option[MdTag]) extends Read {

  assert(baseQualities.length == sequence.length)

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
   * Convenience function (intended for test cases), to construct a Read from unparsed values.
   */
  def apply(
    sequence: String,
    baseQualities: String = "",
    isDuplicate: Boolean = false,
    sampleName: String = "",
    referenceContig: String = "",
    alignmentQuality: Int = -1,
    start: Long = -1L,
    cigarString: String = "",
    mdTagString: String = ""): Read = {

    val sequenceArray = sequence.map(_.toByte).toArray
    val qualityScoresArray = {
      // If no base qualities are set, we set them all to 0.
      if (baseQualities.isEmpty)
        sequenceArray.map(_ => 0.toByte).toSeq.toArray
      else
        baseQualities.map(q => (q - 33).toByte).toArray
    }

    if (referenceContig.isEmpty) {
      UnmappedRead(sequenceArray, qualityScoresArray, isDuplicate, sampleName.intern)
    } else {
      val cigar = TextCigarCodec.getSingleton.decode(cigarString)
      val mdTag = if (mdTagString.isEmpty) None else Some(MdTag(mdTagString, start))
      MappedRead(
        sequenceArray,
        qualityScoresArray,
        isDuplicate,
        sampleName.intern,
        referenceContig,
        alignmentQuality,
        start,
        cigar,
        mdTag)
    }
  }

  /**
   * Convert a SAM tools record into a Read.
   *
   * @param record
   * @return
   */
  def fromSAMRecord(record: SAMRecord): Read = {
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
        record.getReadString.getBytes,
        record.getBaseQualities,
        record.getDuplicateReadFlag,
        sampleName,
        record.getReferenceName.intern,
        record.getMappingQuality,
        record.getAlignmentStart - 1,
        cigar = record.getCigar,
        mdTag = mdTag)

      // We subtract 1 from start, since samtools is 1-based and we're 0-based.
      if (result.unclippedStart != record.getUnclippedStart - 1)
        log.warn("Computed read 'unclippedStart' %d != samtools read end %d.".format(
          result.unclippedStart, record.getUnclippedStart - 1))
      result
    } else {
      val result = UnmappedRead(
        record.getReadString.getBytes,
        record.getBaseQualities,
        record.getDuplicateReadFlag,
        sampleName)
      result
    }
  }

  /**
   * Given a local path to a BAM/SAM file, return a pair: (Array, SequenceDictionary), where the first element is an
   * array of the reads, and the second is a Sequence Dictionary giving info (e.g. length) about the contigs in the BAM.
   */
  def loadReadArrayAndSequenceDictionaryFromBAM(
    filename: String,
    mapped: Boolean = true,
    nonDuplicate: Boolean = true): (ArrayBuffer[Read], SequenceDictionary) = {
    val reader = new SAMFileReader(new java.io.File(filename))
    val sequenceDictionary = SequenceDictionary.fromSAMReader(reader)
    val result = new ArrayBuffer[Read]
    for (item: SAMRecord <- JavaConversions.asScalaIterator(reader.iterator)) {
      if (!nonDuplicate || !item.getDuplicateReadFlag) {
        val read = fromSAMRecord(item)
        if (!mapped || read.isMapped) result += read
      }
    }
    (result, sequenceDictionary)
  }

  /**
   * Given a filename and a spark context, return a pair (RDD, SequenceDictionary), where the first element is an RDD
   * of Reads, and the second element is the Sequence Dictionary giving info (e.g. length) about the contigs in the BAM.
   *
   * @param filename name of file containing reads
   * @param sc spark context
   * @param mapped if true, will filter out non-mapped reads
   * @param nonDuplicate if true, will filter out duplicate reads.
   * @return
   */
  def loadReadRDDAndSequenceDictionaryFromBAM(
    filename: String,
    sc: SparkContext,
    mapped: Boolean = true,
    nonDuplicate: Boolean = true): (RDD[Read], SequenceDictionary) = {

    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filename), sc.hadoopConfiguration)
    val sequenceDictionary = SequenceDictionary.fromSAMHeader(samHeader)

    val samRecords: RDD[(LongWritable, SAMRecordWritable)] =
      sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, AnySAMInputFormat](filename)
    var reads: RDD[Read] = samRecords.map({ case (k, v) => fromSAMRecord(v.get) })
    if (mapped) reads = reads.filter(_.isMapped)
    if (nonDuplicate) reads = reads.filter(read => !read.isDuplicate)
    (reads, sequenceDictionary)
  }

  /** Is the given samtools CigarElement a (hard/soft) clip? */
  def cigarElementIsClipped(element: CigarElement): Boolean = {
    element.getOperator == CigarOperator.SOFT_CLIP || element.getOperator == CigarOperator.HARD_CLIP
  }
}

