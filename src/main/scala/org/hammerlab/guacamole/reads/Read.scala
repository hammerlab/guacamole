package org.hammerlab.guacamole.reads

import htsjdk.samtools._
import org.apache.spark.Logging
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.guacamole.readsets.SampleName
import org.hammerlab.guacamole.util.Bases

/**
 * The fields in the Read trait are common to any read, whether mapped (aligned) or not.
 */
trait Read extends HasSampleId {

  /* The template name. A read, its mate, and any alternate alignments have the same name. */
  def name: String

  /** The nucleotide sequence. */
  def sequence: IndexedSeq[Byte]

  /** The base qualities, phred scaled.  These are numbers, and are NOT character encoded. */
  def baseQualities: IndexedSeq[Byte]

  /** Is this read a duplicate of another? */
  def isDuplicate: Boolean

  /** Is this read mapped? */
  def isMapped: Boolean

  def asMappedRead: Option[MappedRead]

  /** The sample (e.g. "tumor" or "patient3636") name. */
  def sampleName: SampleName

  /** Whether the read failed predefined vendor checks for quality */
  def failedVendorQualityChecks: Boolean

  /** Whether read is from a paired-end library */
  def isPaired: Boolean

}

object Read extends Logging {
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
  def apply(record: SAMRecord, sampleId: Int): Read = {

    val isMapped = (
      !record.getReadUnmappedFlag &&
      record.getReferenceName != null &&
      record.getReferenceIndex >= SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX &&
      record.getAlignmentStart >= 0 &&
      record.getUnclippedStart >= 0)

    val sampleName =
      (if (record.getReadGroup != null && record.getReadGroup.getSample != null)
        record.getReadGroup.getSample
      else
        "default"
      ).intern

    val read =
      if (isMapped) {
        val result =
          MappedRead(
            record.getReadName,
            record.getReadString.getBytes,
            record.getBaseQualities,
            record.getDuplicateReadFlag,
            sampleId,
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
          log.warn(
            "Computed read 'unclippedStart' %d != samtools read end %d.".format(
              result.unclippedStart, record.getUnclippedStart - 1
            )
          )

        result
      } else
        UnmappedRead(
          record.getReadName,
          record.getReadString.getBytes,
          record.getBaseQualities,
          record.getDuplicateReadFlag,
          sampleId,
          sampleName,
          record.getReadFailsVendorQualityCheckFlag,
          record.getReadPairedFlag
        )

    if (record.getReadPairedFlag) {
      val mateAlignment = MateAlignmentProperties(record)
      PairedRead(read, isFirstInPair = record.getFirstOfPairFlag, mateAlignment)
    } else {
      read
    }
  }

  /**
   *
   * Builds a Guacamole Read from and ADAM Alignment Record
   *
   * @param alignmentRecord ADAM Alignment Record (an aligned or unaligned read)
   * @return Mapped or Unmapped Read
   */
  def apply(alignmentRecord: AlignmentRecord, sampleId: Int): Read = {

    val sequence = Bases.stringToBases(alignmentRecord.getSequence)
    val baseQualities = baseQualityStringToArray(alignmentRecord.getQual, sequence.length)

    val referenceContig = alignmentRecord.getContig.getContigName.intern
    val cigar = TextCigarCodec.decode(alignmentRecord.getCigar)

    val read =
      if (alignmentRecord.getReadMapped)
        MappedRead(
          name = alignmentRecord.getReadName,
          sequence = sequence,
          baseQualities = baseQualities,
          isDuplicate = alignmentRecord.getDuplicateRead,
          sampleId = sampleId,
          sampleName = alignmentRecord.getRecordGroupSample.intern(),
          contigName = referenceContig,
          alignmentQuality = alignmentRecord.getMapq,
          start = alignmentRecord.getStart,
          cigar = cigar,
          failedVendorQualityChecks = alignmentRecord.getFailedVendorQualityChecks,
          isPositiveStrand = !alignmentRecord.getReadNegativeStrand,
          alignmentRecord.getReadPaired
        )
      else
        UnmappedRead(
          name = alignmentRecord.getReadName,
          sequence = sequence,
          baseQualities = baseQualities,
          isDuplicate = alignmentRecord.getDuplicateRead,
          sampleId = sampleId,
          sampleName = alignmentRecord.getRecordGroupSample.intern(),
          failedVendorQualityChecks = alignmentRecord.getFailedVendorQualityChecks,
          alignmentRecord.getReadPaired
        )

    if (alignmentRecord.getReadPaired) {
      val mateAlignment = if (alignmentRecord.getMateMapped) Some(
        MateAlignmentProperties(
          contigName = alignmentRecord.getMateContig.getContigName.intern(),
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

}
