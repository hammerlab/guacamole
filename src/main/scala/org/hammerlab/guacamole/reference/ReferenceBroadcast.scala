package org.hammerlab.guacamole.reference

import java.io.File
import java.util.NoSuchElementException

import htsjdk.samtools.reference.FastaSequenceFile
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.hammerlab.guacamole.Bases

case class ReferenceBroadcast(broadcastedContigs: Map[String, Broadcast[Array[Byte]]]) extends ReferenceGenome {

  override def getContig(contigName: String): Array[Byte] = {
    try {
      broadcastedContigs(contigName).value
    } catch {
      case e: NoSuchElementException => throw new ContigNotFound(contigName, broadcastedContigs.keys)
    }
  }

  override def getReferenceBase(contigName: String, locus: Int): Byte = {
    getContig(contigName)(locus)
  }

  override def getReferenceSequence(contigName: String, startLocus: Int, endLocus: Int): Array[Byte] = {
    getContig(contigName).slice(startLocus, endLocus)
  }

}

object ReferenceBroadcast {

  /**
   * Build a map from contig name to broadcasted reference sequence
   * @param fastaPath Local path to a FASTA file
   * @param sc SparkContext
   * @return ReferenceBroadcast which maps contig/chromosome names to broadcasted sequences
   */
  def apply(fastaPath: String, sc: SparkContext): ReferenceBroadcast = {

    val referenceFasta = new FastaSequenceFile(new File(fastaPath), true)
    var nextSequence = referenceFasta.nextSequence()
    val broadcastedSequences = Map.newBuilder[String, Broadcast[Array[Byte]]]
    while (nextSequence != null) {
      val sequenceName = nextSequence.getName
      val sequence = nextSequence.getBases
      Bases.unmaskBases(sequence)
      val broadcastedSequence = sc.broadcast(sequence)

      broadcastedSequences += ((sequenceName, broadcastedSequence))

      nextSequence = referenceFasta.nextSequence()
    }

    ReferenceBroadcast(broadcastedSequences.result)
  }

}

case class ContigNotFound(contigName: String, availableContigs: Iterable[String])
  extends Exception(s"Contig $contigName does not exist in the current reference. Available contigs are ${availableContigs.mkString(",")}")

