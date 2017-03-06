package org.hammerlab.guacamole.reads

import htsjdk.samtools.TextCigarCodec
import org.hammerlab.genomics.bases.{ Bases, BasesUtil }
import org.hammerlab.genomics.reference.test.LocusUtil
import org.hammerlab.genomics.reference.{ ContigName, Locus }
import org.hammerlab.guacamole.readsets.SampleId

trait ReadsUtil
  extends BasesUtil
    with LocusUtil {

  /**
   * Convenience function to construct a Read from unparsed values.
   */
  private def read(sequence: Bases,
                   name: String,
                   baseQualities: String = "",
                   isDuplicate: Boolean = false,
                   sampleId: SampleId = 0,
                   contigName: ContigName = "",
                   alignmentQuality: Int = -1,
                   start: Locus = -1L,
                   cigarString: String = "",
                   failedVendorQualityChecks: Boolean = false,
                   isPositiveStrand: Boolean = true,
                   isPaired: Boolean = true) = {

    val qualityScoresArray = Read.baseQualityStringToArray(baseQualities, sequence.length)

    val cigar = TextCigarCodec.decode(cigarString)
    MappedRead(
      name,
      sequence,
      qualityScoresArray,
      isDuplicate,
      sampleId,
      contigName,
      alignmentQuality,
      start,
      cigar,
      failedVendorQualityChecks,
      isPositiveStrand,
      isPaired
    )
  }

  def makeRead(sequence: Bases,
               cigar: String,
               qualityScores: Seq[Int]): MappedRead =
    makeRead(sequence, cigar, qualityScores = Some(qualityScores))

  def makeRead(sequence: Bases,
               cigar: String,
               start: Locus,
               chr: ContigName,
               qualityScores: Seq[Int]): MappedRead =
    makeRead(
      sequence,
      cigar,
      start,
      chr,
      qualityScores = Some(qualityScores)
    )

  def makeRead(sequence: Bases,
               cigar: String = "",
               start: Locus = 1,
               chr: ContigName = "chr1",
               sampleId: SampleId = 0,
               qualityScores: Option[Seq[Int]] = None,
               alignmentQuality: Int = 30): MappedRead = {

    val qualityScoreString =
      qualityScores match {
        case Some(qualityScores) ⇒ qualityScores.map(q ⇒ q + 33).map(_.toChar).mkString
        case _ ⇒ "@" * sequence.length
      }

    read(
      sequence,
      name = "read1",
      cigarString = cigar,
      start = start,
      contigName = chr,
      sampleId = sampleId,
      baseQualities = qualityScoreString,
      alignmentQuality = alignmentQuality
    )
  }

  def makePairedRead(chr: ContigName = "chr1",
                     start: Locus = 1,
                     alignmentQuality: Int = 30,
                     isPositiveStrand: Boolean = true,
                     isMateMapped: Boolean = false,
                     mateReferenceContig: Option[String] = None,
                     mateStart: Option[Long] = None,
                     isMatePositiveStrand: Boolean = false,
                     sequence: Bases = "ACTGACTGACTG",
                     cigar: String = "12M",
                     inferredInsertSize: Option[Int]): PairedRead[MappedRead] = {

    val qualityScoreString = "@" * sequence.length

    PairedRead(
      read(
        sequence,
        name = "read1",
        cigarString = cigar,
        start = start,
        contigName = chr,
        isPositiveStrand = isPositiveStrand,
        baseQualities = qualityScoreString,
        alignmentQuality = alignmentQuality,
        isPaired = true
      ),
      isFirstInPair = true,
      mateAlignmentProperties =
        if (isMateMapped)
          Some(
            MateAlignmentProperties(
              mateReferenceContig.get,
              mateStart.get,
              inferredInsertSize = inferredInsertSize,
              isPositiveStrand = isMatePositiveStrand
            )
          )
        else
          None
    )
  }

  def makeReads(contigName: ContigName, reads: (String, String, Int)*): Seq[MappedRead] =
    for {
      (sequence, cigar, start) ← reads
    } yield
      makeRead(sequence, cigar, start, chr = contigName)

  def makeReads(reads: (String, String, Int)*): Seq[MappedRead] =
    for {
      (sequence, cigar, start) ← reads
    } yield
      makeRead(sequence, cigar, start)
}
