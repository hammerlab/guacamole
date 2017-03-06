package org.hammerlab.guacamole.reference

import java.io.File
import java.util.NoSuchElementException

import grizzled.slf4j.Logging
import htsjdk.samtools.reference.FastaSequenceFile
import org.apache.spark.SparkContext
import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.genomics.loci.parsing.{ LociRange, LociRanges, ParsedLociRange }
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.{ ContigName, ContigSequence, Locus, NumLoci }

import scala.collection.mutable

case class ReferenceBroadcast(broadcastedContigs: Map[ContigName, ContigSequence],
                              source: Option[String])
  extends ReferenceGenome {

  override def getContig(contigName: ContigName): ContigSequence =
    try {
      broadcastedContigs(contigName)
    } catch {
      case _: NoSuchElementException ⇒
        throw ContigNotFound(contigName, broadcastedContigs.keys)
    }

  override def getReferenceBase(contigName: ContigName, locus: Locus): Base =
    getContig(contigName)(locus)

  override def getReferenceSequence(contigName: ContigName, startLocus: Locus, length: Int): Bases =
    getContig(contigName).slice(startLocus, length)
}

object ReferenceBroadcast extends Logging {

  /**
   * Read a regular fasta file
   * @param fastaPath local path to fasta
   * @param sc the spark context
   * @return a ReferenceBroadcast instance containing ArrayBackedReferenceSequence objects.
   */
  def readFasta(fastaPath: String, sc: SparkContext): ReferenceBroadcast = {
    val referenceFasta = new FastaSequenceFile(new File(fastaPath), true)
    var nextSequence = referenceFasta.nextSequence()
    val broadcastedSequences = Map.newBuilder[ContigName, ContigSequence]
    while (nextSequence != null) {
      val contigName = nextSequence.getName
      val sequence = nextSequence.getBases
      info(s"Broadcasting contig: $contigName")
      val broadcastedSequence = ArrayBackedReferenceSequence(contigName, sc.broadcast(sequence))
      broadcastedSequences += ((contigName, broadcastedSequence))
      nextSequence = referenceFasta.nextSequence()
    }
    ReferenceBroadcast(broadcastedSequences.result, Some(fastaPath))
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
   * @param sc the spark context
   * @return a ReferenceBroadcast instance containing MapBackedReferenceSequence objects
   */
  def readPartialFasta(fastaPath: String, sc: SparkContext): ReferenceBroadcast = {
    val raw = readFasta(fastaPath, sc)

    val result = mutable.HashMap[ContigName, mutable.HashMap[Locus, Base]]()

    val contigLengths = mutable.HashMap[ContigName, NumLoci]()

    for {
      (regionDescription, broadcastSequence) <- raw.broadcastedContigs
      length = broadcastSequence.length
      sequence = broadcastSequence.slice(Locus(0), length.toInt)
    } {
      regionDescription.name.split("/").map(_.trim).toList match {
        case lociStr :: contigLengthStr :: Nil ⇒
          val contigLength: NumLoci = contigLengthStr.toLong

          val loci =
            ParsedLociRange(lociStr) match {
              case Some(LociRange(contigName, start, endOpt)) ⇒
                val parsedLoci = LociRanges(LociRange(contigName, start, endOpt))
                LociSet(parsedLoci, Map(contigName -> contigLength))
              case _ ⇒
                throw new IllegalArgumentException(s"Bad loci range: $lociStr")
            }

          if (loci.contigs.length != 1) {
            throw new IllegalArgumentException(s"Region must have 1 contig for partial fasta: $lociStr")
          }

          val contig = loci.contigs.head
          val regionLength = contig.count
          if (regionLength != length) {
            throw new IllegalArgumentException(
              "In partial fasta, region %s is length %,d but its sequence is length %,d".format(
                lociStr, regionLength.num, sequence.length
              )
            )
          }

          val maxRegionEnd = NumLoci(contig.ranges.map(_.end).max)
          if (maxRegionEnd > contigLength) {
            throw new IllegalArgumentException(
              "In partial fasta, region %s (max=%,d) exceeds contig length %,d".format(
                lociStr, maxRegionEnd.num, contigLength.num
              )
            )
          }

          if (contigLengths.getOrElseUpdate(contig.name, contigLength) != contigLength) {
            throw new IllegalArgumentException(
              s"In partial fasta, contig lengths for $contig are inconsistent ($contigLength vs ${contigLengths(contig.name)})"
            )
          }

          val sequenceMap = result.getOrElseUpdate(contig.name, mutable.HashMap[Locus, Base]())
          for {
            (locus, base) <- contig.iterator.zip(sequence.iterator)
          } {
            sequenceMap.update(locus, base)
          }

        case _ ⇒
          throw new IllegalArgumentException(
            s"Invalid sequence name for partial fasta: $regionDescription. Are you sure this is a partial fasta, not a regular fasta?"
          )
      }
    }

    new ReferenceBroadcast(
      (for {
        (contigName, baseMap) <- result
        contigLength = contigLengths(contigName)
        baseMapBroadcast = sc.broadcast(baseMap.toMap)
      } yield
        contigName -> MapBackedReferenceSequence(contigName, contigLength, baseMapBroadcast)
      ).toMap,
      Some(fastaPath)
    )
  }

  /**
   * Load a ReferenceBroadcast, caching the result.
   *
   * @param fastaPath Local path to a FASTA file
   * @param sc the spark context
   * @param partialFasta is this a "partial fasta"? Partial fastas are used in tests to load only a subset of the
   *                     reference. In production runs this will usually be false.
   * @return ReferenceBroadcast which maps contig/chromosome names to broadcasted sequences
   */
  def apply(fastaPath: String, sc: SparkContext, partialFasta: Boolean = false): ReferenceBroadcast =
    if (partialFasta)
      readPartialFasta(fastaPath, sc)
    else
      readFasta(fastaPath, sc)

  def apply(broadcastedContigs: Map[ContigName, ContigSequence]): ReferenceBroadcast =
    ReferenceBroadcast(broadcastedContigs, source = None)
}

case class ContigNotFound(contigName: ContigName, availableContigs: Iterable[ContigName])
  extends Exception(
    s"Contig $contigName does not exist in the current reference. Available contigs are ${availableContigs.mkString(",")}"
  )

