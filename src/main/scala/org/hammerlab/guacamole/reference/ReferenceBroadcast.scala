package org.hammerlab.guacamole.reference

import java.io.File
import java.util.NoSuchElementException

import htsjdk.samtools.reference.FastaSequenceFile
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.util.Bases

import scala.collection.mutable

case class ReferenceBroadcast(broadcastedContigs: Map[String, ContigSequence],
                              source: Option[String])
  extends ReferenceGenome {

  override def getContig(contigName: String): ContigSequence =
    try {
      broadcastedContigs(contigName)
    } catch {
      case _: NoSuchElementException =>
        throw ContigNotFound(contigName, broadcastedContigs.keys)
    }

  override def getReferenceBase(contigName: String, locus: Int): Byte =
    getContig(contigName)(locus)

  override def getReferenceSequence(contigName: String, startLocus: Int, endLocus: Int): Array[Byte] =
    getContig(contigName).slice(startLocus, endLocus)
}

object ReferenceBroadcast {
  /**
   * The standard ContigSequence implementation, which is an Array of bases.
   *
   * TODO: Array's can't be more than 2^32 long, use 2bit instead?
   */
  case class ArrayBackedReferenceSequence(wrapped: Broadcast[Array[Byte]]) extends ContigSequence {
    def apply(index: Int): Byte = wrapped.value(index)
    def slice(start: Int, end: Int): Array[Byte] = wrapped.value.slice(start, end)
    def length: Int = wrapped.value.length
  }

  object ArrayBackedReferenceSequence {
    /** Create an ArrayBackedReferenceSequence from a string. This is a convenience method intended for tests. */
    def apply(sc: SparkContext, sequence: String): ArrayBackedReferenceSequence =
      ArrayBackedReferenceSequence(sc.broadcast(Bases.stringToBases(sequence).toArray))
  }

  /**
   * A ContigSequence implementation that uses a Map to store only a subset of bases. This is what you get if you load
   * a "partial fasta". This is used in tests.
   * @param length
   * @param wrapped
   */
  case class MapBackedReferenceSequence(length: Int, wrapped: Broadcast[Map[Int, Byte]]) extends ContigSequence {
    def apply(index: Int): Byte = wrapped.value.getOrElse(index, Bases.N)
    def slice(start: Int, end: Int): Array[Byte] = (start until end).map(apply).toArray
  }

  val cache = mutable.HashMap.empty[String, (SparkContext, ReferenceBroadcast)]

  /**
   * Read a regular fasta file
   * @param fastaPath local path to fasta
   * @param sc the spark context
   * @return a ReferenceBroadcast instance containing ArrayBackedReferenceSequence objects.
   */
  def readFasta(fastaPath: String, sc: SparkContext): ReferenceBroadcast = {
    val referenceFasta = new FastaSequenceFile(new File(fastaPath), true)
    var nextSequence = referenceFasta.nextSequence()
    val broadcastedSequences = Map.newBuilder[String, ContigSequence]
    while (nextSequence != null) {
      val sequenceName = nextSequence.getName
      val sequence = nextSequence.getBases
      Bases.unmaskBases(sequence)
      val broadcastedSequence = ArrayBackedReferenceSequence(sc.broadcast(sequence))
      broadcastedSequences += ((sequenceName, broadcastedSequence))
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
    val result = mutable.HashMap[ContigName, mutable.HashMap[Int, Byte]]()
    val contigLengths = mutable.HashMap[ContigName, Int]()

    for {
      (regionDescription, broadcastSequence) <- raw.broadcastedContigs
      sequence = broadcastSequence.slice(0, broadcastSequence.length)
    } {
      regionDescription.split("/").map(_.trim).toList match {
        case lociStr :: contigLengthStr :: Nil =>
          val contigLength = contigLengthStr.toInt
          val region = LociSet(lociStr)
          if (region.contigs.length != 1) {
            throw new IllegalArgumentException(s"Region must have 1 contig for partial fasta: $lociStr")
          }

          val contig = region.contigs.head
          val regionLength = contig.count
          if (regionLength != sequence.length) {
            throw new IllegalArgumentException(
              "In partial fasta, region %s is length %,d but its sequence is length %,d".format(
                lociStr, regionLength, sequence.length
              )
            )
          }

          val maxRegion = contig.ranges.map(_.end).max
          if (maxRegion > contigLength) {
            throw new IllegalArgumentException(
              "In partial fasta, region %s (max=%,d) exceeds contig length %,d".format(
                lociStr, maxRegion, contigLength
              )
            )
          }

          if (contigLengths.getOrElseUpdate(contig.name, contigLength) != contigLength) {
            throw new IllegalArgumentException(
              "In partial fasta, contig lengths for %s are inconsistent (%d vs %d)".format(
                contig, contigLength, contigLengths(contig.name)
              )
            )
          }

          val sequenceMap = result.getOrElseUpdate(contig.name, mutable.HashMap[Int, Byte]())
          for {
            (locus, base) <- contig.iterator.zip(sequence.iterator)
          } {
            sequenceMap.update(locus.toInt, base)
          }

        case _ =>
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
        contigName -> MapBackedReferenceSequence(contigLength, baseMapBroadcast)
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
  def apply(fastaPath: String, sc: SparkContext, partialFasta: Boolean = false): ReferenceBroadcast = {
    val lookup = cache.get(fastaPath)
    if (lookup.exists(_._1 == sc)) {
      lookup.get._2
    } else {
      val result = if (partialFasta) readPartialFasta(fastaPath, sc) else readFasta(fastaPath, sc)
      // Currently we keep only one item in the cache at a time.
      cache.clear()
      cache.put(fastaPath, (sc, result))
      result
    }
  }

  def apply(broadcastedContigs: Map[String, ContigSequence]): ReferenceBroadcast =
    ReferenceBroadcast(broadcastedContigs, source = None)
}

case class ContigNotFound(contigName: String, availableContigs: Iterable[String])
  extends Exception(
    s"Contig $contigName does not exist in the current reference. Available contigs are ${availableContigs.mkString(",")}"
  )

