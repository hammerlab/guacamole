package org.hammerlab.guacamole.reference

import java.io.File
import java.util.NoSuchElementException

import htsjdk.samtools.reference.FastaSequenceFile
import org.hammerlab.guacamole.{Bases, LociSet}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class ReferenceGenome(contigs: Map[String, ContigSequence]) {
  def getContig(contigName: String): ContigSequence = {
    try {
      contigs(contigName)
    } catch {
      case e: NoSuchElementException =>
        throw new ContigNotFound(contigName, contigs.keys)
    }
  }

  def getReferenceBase(contigName: String, locus: Int): Byte = {
    getContig(contigName)(locus)
  }

  def getReferenceSequence(contigName: String, startLocus: Int, endLocus: Int): ContigSubsequence = {
    getContig(contigName).slice(startLocus, endLocus)
  }
}

object ReferenceGenome {

  protected val cache = mutable.HashMap.empty[String, ReferenceGenome]

  /**
   * Read a regular fasta file
   *
   * @param fastaPath local path to fasta
   * @return a ReferenceBroadcast instance containing ContigSequence.
   */
  def readFasta(fastaPath: String): ReferenceGenome = {
    val referenceFasta = new FastaSequenceFile(new File(fastaPath), true)
    var nextSequence = referenceFasta.nextSequence()
    val sequences = Map.newBuilder[String, ContigSequence]
    while (nextSequence != null) {
      val sequenceName = nextSequence.getName
      val sequence = nextSequence.getBases
      Bases.unmaskBases(sequence)
      sequences += ((sequenceName, sequence))
      nextSequence = referenceFasta.nextSequence()
    }
    ReferenceGenome(sequences.result)
  }

  /**
   * Read a "partial fasta" from the given path.
   *
   * A "partial fasta" is a fasta file where the reference names look like "chr1:9242255-9242454/249250621". That gives
   * the contig name, the start and end locus, and the total contig size. The associated sequence in the file gives the
   * reference sequence for just the sites between start and end.
   *
   * Partial fastas are used for testing to avoid distributing full reference genomes.
   *
   * @param fastaPath local path to partial fasta
   * @return a ReferenceBroadcast instance containing MapBackedReferenceSequence objects
   */
  private def readPartialFasta(fastaPath: String): ReferenceGenome = {
    val raw = ReferenceGenome.readFasta(fastaPath)
    val result = mutable.HashMap[String, ArrayBuffer[(Int, Byte)]]()
    val contigLengths = mutable.HashMap[String, Int]()

    raw.contigs.foreach({
      case (regionDescription, sequence) => {

        val pieces = regionDescription.split("/").map(_.trim)
        if (pieces.length != 2) {
          throw new IllegalArgumentException(
            s"Invalid sequence name for partial fasta: $regionDescription. Are you sure this is a partial fasta, not a regular fasta?"
          )
        }
        val contigLength = pieces(1).toInt
        val region = LociSet.parse(pieces(0)).result
        if (region.contigs.length != 1) {
          throw new IllegalArgumentException(s"Region must have 1 contig for partial fasta: ${pieces(0)}")
        }
        val contig = region.contigs.head
        val regionLength = region.onContig(contig).count
        if (regionLength != sequence.length) {
          throw new IllegalArgumentException(
            "In partial fasta, region %s is length %,d but its sequence is length %,d".format(
              pieces(0), regionLength, sequence.length
            )
          )
        }
        val maxRegion = region.onContig(contig).ranges.map(_.end).max
        if (maxRegion > contigLength) {
          throw new IllegalArgumentException(
            "In partial fasta, region %s (max=%,d) exceeds contig length %,d".format(
              pieces(0), maxRegion, contigLength
            )
          )
        }
        if (contigLengths.getOrElseUpdate(contig, contigLength) != contigLength) {
          throw new IllegalArgumentException(
            "In partial fasta, contig lengths for %s are inconsistent (%d vs %d)".format(
              contig, contigLength, contigLengths(contig)
            )
          )
        }
        val sequenceMap = result.getOrElseUpdate(contig, ArrayBuffer[(Int, Byte)]())
        for {
          (locus, base) ← region.onContig(contig).iterator.zip(sequence.toIterator)
        } {
          sequenceMap.append((locus.toInt, base))
        }
      }
    })

    new ReferenceGenome(
      (for {
        (contig, bases) ← result
      } yield {
        contig →
          MapBackedReferenceSequence(
            contigLengths(contig),
            bases
          )
      }).toMap
    )
  }

  /**
   * Load a ReferenceBroadcast, caching the result.
   *
   * @param fastaPath Local path to a FASTA file
   * @return ReferenceBroadcast which maps contig/chromosome names to broadcasted sequences
   */
  def apply(fastaPath: String, partialFasta: Boolean = false): ReferenceGenome = {
    cache.getOrElseUpdate(
      fastaPath,
      if (partialFasta)
        readPartialFasta(fastaPath)
      else
        readFasta(fastaPath)
    )
  }
}

case class ContigNotFound(contigName: String, availableContigs: Iterable[String])
  extends Exception(
    s"Contig $contigName does not exist in the current reference. Available contigs are ${availableContigs.mkString(",")}"
  )
