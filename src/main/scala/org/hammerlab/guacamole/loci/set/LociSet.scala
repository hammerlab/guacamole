package org.hammerlab.guacamole.loci.set

import java.lang.{Long => JLong}

import com.google.common.collect.{Range => JRange}
import org.hammerlab.guacamole.readsets.ContigLengths
import org.hammerlab.guacamole.reference.{ContigName, Locus, NumLoci, ReferenceRegion}
import org.hammerlab.guacamole.strings.TruncatedToString

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

/**
 * An immutable collection of genomic regions on any number of contigs.
 *
 * Used, for example, to keep track of what loci to call variants at.
 *
 * Since contiguous genomic intervals are a common case, this is implemented with sets of (start, end) intervals.
 *
 * All intervals are half open: inclusive on start, exclusive on end.
 *
 * @param map A map from contig-name to Contig, which is a set or genomic intervals as described above.
 */
case class LociSet(private val map: SortedMap[ContigName, Contig]) extends TruncatedToString {

  /** The contigs included in this LociSet with a nonempty set of loci. */
  @transient lazy val contigs = map.values.toArray

  /** The number of loci in this LociSet. */
  @transient lazy val count: NumLoci = contigs.map(_.count).sum

  def isEmpty = map.isEmpty
  def nonEmpty = map.nonEmpty

  /** Given a contig name, returns a [[Contig]] giving the loci on that contig. */
  def onContig(name: ContigName): Contig = map.getOrElse(name, Contig(name))

  /** Build a truncate-able toString() out of underlying contig pieces. */
  def stringPieces: Iterator[String] = contigs.iterator.flatMap(_.stringPieces)

  def intersects(region: ReferenceRegion): Boolean =
    onContig(region.contigName).intersects(region.start, region.end)

  /**
   * Split the LociSet into two sets, where the first one has `numToTake` loci, and the second one has the
   * remaining loci.
   *
   * @param numToTake number of elements to take. Must be <= number of elements in the map.
   */
  def take(numToTake: NumLoci): (LociSet, LociSet) = {
    assume(numToTake <= count, s"Can't take $numToTake loci from a set of size $count.")

    // Optimize for taking none or all:
    if (numToTake == 0) {
      (LociSet(), this)
    } else if (numToTake == count) {
      (this, LociSet())
    } else {

      val first = new Builder
      val second = new Builder
      var remaining = numToTake
      var doneTaking = false

      for {
        contig <- contigs
      } {
        if (doneTaking) {
          second.add(contig)
        } else if (contig.count < remaining) {
          first.add(contig)
          remaining -= contig.count
        } else {
          val (takePartialContig, remainingPartialContig) = contig.take(remaining)
          first.add(takePartialContig)
          second.add(remainingPartialContig)
          doneTaking = true
        }
      }

      val (firstSet, secondSet) = (first.result, second.result)
      assert(firstSet.count == numToTake)
      assert(firstSet.count + secondSet.count == count)
      (firstSet, secondSet)
    }
  }
}

object LociSet {
  /** An empty LociSet. */
  def apply(): LociSet = LociSet(TreeMap.empty[ContigName, Contig])

  def all(contigLengths: ContigLengths) = LociParser.all.result(contigLengths)

  def apply(lociStr: String): LociSet = LociParser(lociStr).result

  /**
   * These constructors build a LociSet directly from Contigs.
   *
   * They operate on an Iterator so that transformations to the data happen in one pass.
   */
  private[set] def fromContigs(contigs: Iterable[Contig]): LociSet = fromContigs(contigs.iterator)
  private[set] def fromContigs(contigs: Iterator[Contig]): LociSet =
    LociSet(
      TreeMap(
        contigs
          .filterNot(_.isEmpty)
          .map(contig => contig.name -> contig)
          .toSeq: _*
      )
    )

  def apply(contigs: Iterable[(ContigName, Locus, Locus)]): LociSet =
    LociSet.fromContigs({
      (for {
        (name, start, end) <- contigs
        range = JRange.closedOpen[JLong](start, end)
        if !range.isEmpty
      } yield {
        name -> range
      })
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .map(Contig(_))
    })
}
