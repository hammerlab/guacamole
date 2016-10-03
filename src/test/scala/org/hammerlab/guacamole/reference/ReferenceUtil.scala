package org.hammerlab.guacamole.reference

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.util.Bases.stringToBases

import scala.collection.mutable

trait ReferenceUtil {

  /**
   * Make a ReferenceBroadcast containing the specified sequences to be used in tests.
   *
   * @param contigStartSequences tuples of (contig name, start, reference sequence) giving the desired sequences
   * @param contigLengths total length of each contigs (for simplicity all contigs are assumed to have the same length)
   * @return a map acked ReferenceBroadcast containing the desired sequences
   */
  def makeReference(sc: SparkContext,
                    contigLengths: Int,
                    contigStartSequences: (ContigName, Int, String)*): ReferenceBroadcast = {

    val basesMap = mutable.HashMap[String, mutable.Map[Int, Byte]]()

    for {
      (contigName, start, sequence) <- contigStartSequences
    } {
      val contigBasesMap = basesMap.getOrElseUpdate(contigName, mutable.Map())
      for {
        (base, offset) <- stringToBases(sequence).zipWithIndex
        locus = start + offset
      } {
        contigBasesMap(locus) = base
      }
    }

    val contigsMap =
      for {
        (contigName, contigBasesMap) <- basesMap.toMap
      } yield
        contigName ->
          MapBackedReferenceSequence(
            contigName,
            contigLengths,
            sc.broadcast(contigBasesMap.toMap)
          )

    new ReferenceBroadcast(contigsMap, source = Some("test_values"))
  }

  def makeReference(sc: SparkContext, contigStartSequences: (ContigName, Int, String)*): ReferenceBroadcast =
    makeReference(sc, 1000, contigStartSequences: _*)

  def makeReference(sc: SparkContext, contigName: ContigName, start: Int, sequence: String): ReferenceBroadcast =
    makeReference(sc, (contigName, start, sequence))
}
