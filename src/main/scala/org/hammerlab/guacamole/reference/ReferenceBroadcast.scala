package org.hammerlab.guacamole.reference

import java.io.File
import java.util.NoSuchElementException

import htsjdk.samtools.reference.FastaSequenceFile
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

case class ReferenceBroadcast(broadcastedContigs: Map[String, Broadcast[Array[Byte]]]) {

  /**
   * Retrieve a full contig sequence
   * @param contigName contig/chromosome to retrieve reference sequence from
   * @return Full sequence associated with the contig
   */
  def getContig(contigName: String): Array[Byte] = {
    try {
      broadcastedContigs(contigName).value
    } catch {
      case e: NoSuchElementException => throw new ContigNotFound(contigName, broadcastedContigs.keys)
    }
  }

  /**
   * Retrieve a reference base on a given contig at a given locus
   * @param contigName contig/chromosome to retrieve reference sequence from
   * @param locus position in the sequence to retrieve
   * @return Base at the given reference position
   */
  def getReferenceBase(contigName: String, locus: Int): Byte = {
    getContig(contigName)(locus)
  }

  /**
   * *
   * @param contigName contig/chromosome to retrieve reference sequence from
   * @param startLocus 0-based inclusive start of the subsequence
   * @param endLocus 0-based exclusive end of the subsequence
   * @return
   */
  def getReferenceSequence(contigName: String, startLocus: Int, endLocus: Int): Array[Byte] = {
    getContig(contigName).slice(startLocus, endLocus)
  }

}

object ReferenceBroadcast {

  def apply(fastaPath: String, sc: SparkContext): ReferenceBroadcast = {

    val referenceFasta = new FastaSequenceFile(new File(fastaPath), true)
    var nextSequence = referenceFasta.nextSequence()
    val broadcastedSequences = Map.newBuilder[String, Broadcast[Array[Byte]]]
    while (nextSequence != null) {
      val sequenceName = nextSequence.getName
      val sequence = nextSequence.getBases
      val broadcastedSequence = sc.broadcast(sequence)

      broadcastedSequences += ((sequenceName, broadcastedSequence))

      nextSequence = referenceFasta.nextSequence()
    }

    ReferenceBroadcast(broadcastedSequences.result)
  }

}

case class ContigNotFound(contigName: String, availableContigs: Iterable[String])
  extends Exception(s"Contig $contigName does not exist in the current reference. Available contigs are ${availableContigs.mkString(",")}")

