package org.hammerlab.guacamole.reference

import org.apache.spark.SparkContext
import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.genomics.reference.test.LocusUtil
import org.hammerlab.genomics.reference.{ ContigName, Locus, NumLoci }

import scala.collection.mutable

trait ReferenceUtil
  extends BasesUtil
    with LocusUtil {

  def sc: SparkContext

  /**
   * Make a ReferenceBroadcast containing the specified sequences to be used in tests.
   *
   * @param contigStartSequences tuples of (contig name, start, reference sequence) giving the desired sequences
   * @param contigLengths total length of each contigs (for simplicity all contigs are assumed to have the same length)
   * @return a map acked ReferenceBroadcast containing the desired sequences
   */
  def makeReference(contigLengths: Int,
                    contigStartSequences: (ContigName, Locus, Bases)*): ReferenceBroadcast = {

    val basesMap = mutable.HashMap[ContigName, mutable.Map[Locus, Base]]()

    for {
      (contigName, start, sequence) ← contigStartSequences
    } {
      val contigBasesMap = basesMap.getOrElseUpdate(contigName, mutable.Map())
      for {
        (base, offset) ← sequence.zipWithIndex
        locus = start + offset
      } {
        contigBasesMap(locus) = base
      }
    }

    val contigsMap =
      for {
        (contigName, contigBasesMap) ← basesMap.toMap
      } yield
        contigName →
          MapBackedReferenceSequence(
            contigName,
            NumLoci(contigLengths),
            sc.broadcast(contigBasesMap.toMap)
          )

    new ReferenceBroadcast(contigsMap, source = Some("test_values"))
  }

  implicit def convertTuple(t: (String, Int, String)): (ContigName, Locus, Bases) = (t._1, t._2, t._3)

  def makeReference(contigStartSequences: (ContigName, Locus, Bases)*): ReferenceBroadcast =
    makeReference(1000, contigStartSequences: _*)

  def makeReference(contigName: ContigName, start: Locus, sequence: Bases): ReferenceBroadcast =
    makeReference((contigName, start, sequence))
}
