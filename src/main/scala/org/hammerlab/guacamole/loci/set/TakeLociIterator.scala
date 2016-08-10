package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.readsets.iterator.{ContigCoverageIterator, ContigsIterator}
import org.hammerlab.guacamole.reference.{ContigIterator, HasLocus, Interval, Locus, Position}
import org.hammerlab.magic.iterator.SimpleBufferedIterator

import scala.collection.mutable.ArrayBuffer

/**
 * Given [[Position]]s and their corresponding [[Coverage]]s, emit a sequence of [[LociSet]]s such that no more than
 * [[maxRegionsPerPartition]] regions overlap each [[LociSet]].
 *
 * The [[LociSet]]s are built greedily, incorporating sequential loci until they reach one that would put them over the
 * [[maxRegionsPerPartition]] limit.
 *
 * @param it Iterator of [[(Position, Coverage)]] tuples, e.g. a [[ContigCoverageIterator]].
 * @param maxRegionsPerPartition Maximum regions allowed to overlap each [[LociSet]].
 */
class TakeLociIterator(it: BufferedIterator[(Position, Coverage)],
                       maxRegionsPerPartition: Int)
  extends SimpleBufferedIterator[LociSet] {

  // Segment the input into per-contig Iterators.
  val contigsCoverages = ContigsIterator.byKey(it)

  override def _advance: Option[LociSet] = {

    var curNumRegions = 0

    // Buffer of Contigs to include in the next LociSet.
    val contigs = ArrayBuffer[Contig]()

    // Build a LociSet from the `contigs` buffer.
    def result: Option[LociSet] =
      if (contigs.nonEmpty)
        Some(
          LociSet.fromContigs(contigs)
        )
      else
        None

    while (contigsCoverages.hasNext) {
      // A given contig might end up straddling the boundary between two LociSets, spilling some loci into the LociSet
      // following this one, so we don't pop it from the contigs iterator yet.
      val (_, contigCoverages) = contigsCoverages.head

      // Attempt to take the maximum number of remaining regions' worth of loci from the current contig.
      takeLoci(
        contigCoverages,
        maxRegionsPerPartition - curNumRegions,
        maxRegionsPerPartition
      ) match {
        case None =>

          /**
           * If we failed to get a Contig from `takeLoci` then one or both of the following are true:
           *
           *   1. the current contig has remaining eligible loci that don't fit in the current LociSet, but would fit in
           *      a fresh LociSet.
           *   2. the current contig is exhausted (though room may remain in this LociSet for additional loci from a
           *      subsequent contig), or.
           *
           * In the former case, we return the current LociSet so that the next one can pick up where it left off.
           *
           * In the latter case, we advance to the next contig and attempt to continue building the current LociSet.
           */
          if (contigCoverages.hasNext)
            return result
          else
            contigsCoverages.next()

        case Some((numRegions, contig)) =>
          /**
           * If we successfully obtained some loci from this contig, add them to our buffer.
           *
           * Rather than try to decide on whether the current contig stopped due to condition 1. or 2. from above, we
           * simply leave the iterators as they are and iterate through the loop here once more, which will fail to pick
           * up any additional loci, leaving the code above to differentiate between cases 1. and 2.
           */
          curNumRegions += numRegions
          contigs += contig
      }
    }

    // We've run through the final contig while building the current LociSet; return what we have.
    result
  }

  /**
   * Take loci from a contig-restricted iterator of loci-keyed [[Coverage]] objects until a maximum number of covering
   * regions has been reached (or the end of the contig).
   *
   * @param it Contig-restricted iterator of loci with corresponding [[Coverage]] stats.
   * @param numRegions Take as many loci as possible without exceeding this number of covering regions.
   * @param dropAbove Loci with greater than this depth are skipped over.
   * @return The number of covering regions, and a [[Contig]] with the taken loci ranges.
   */
  def takeLoci(it: ContigIterator[(HasLocus, Coverage)],
            numRegions: Int,
            dropAbove: Int): Option[(Int, Contig)] = {

    // Accumulate intervals on this contig here.
    val intervals = ArrayBuffer[Interval]()

    // Track the total number of regions accumulated.
    var curNumRegions = 0

    // Endpoints of the interval currently being constructed.
    var curIntervalOpt: Option[(Locus, Locus)] = None

    // If any Intervals have been found, build them into a [[Contig]] and return it, as well as the number of covering
    // regions.
    def result: Option[(Int, Contig)] =
    if (intervals.isEmpty)
      None
    else
      Some(
        curNumRegions ->
          Contig(
            it.contigName,
            intervals.map(_.toJavaRange)
          )
      )

    // Flush the in-progress Interval `curIntervalOpt` to the `intervals` buffer, if one exists.
    def maybeAddInterval(): Unit = {
      curIntervalOpt.foreach(intervals += Interval(_))
      curIntervalOpt = None
    }

    // Return from within the loop, or when this contig's eligible loci have been exhausted.
    while(it.hasNext) {

      // Peek at the first [[Locus]] and corresponding [[Coverage]]; in some situations we want to leave them at the
      // head of the iterator, e.g. if they don't fit in the current Contig/LociSet but would fit in a fresh one, in
      // which case we finalize the current one and begin a new one.
      val (HasLocus(locus), Coverage(depth, starts)) = it.head

      // If this locus is the start of this Contig's contributions to the caller's in-progress LociSet (which is the
      // case iff `curNumRegions == 0`), then this locus contributes `depth` regions to the total (because regions that
      // start before and overlap `locus` must be counted); otherwise, this locus only brings in `starts` new regions.
      val newRegions =
      if(curNumRegions == 0)
        depth
      else
        starts

      if (curNumRegions + newRegions > numRegions) {
        // If the current locus pushed us over the maximum allowed `numRegions`, add the current in-progress Interval.
        maybeAddInterval()

        if (newRegions > dropAbove)
          // If this locus exceeds the `dropAbove` threshold, no LociSet will be able to incorporate it, so we just skip
          // it (and any similar loci that follow it) until we reach a locus that is eligible for incorporation, either
          // in this Contig or a fresh one.
          it.next()
        else
          // This locus has a valid depth and can be incorporated, but the current Contig/LociSet have too little
          // remaining room, so return this Contig as it currently stands (which will also finish off the parent
          // LociSet), and rely on the next LociSet's first Contig to incorporate this locus; in particular, we omit a
          // `it.next()` call here to not consume this locus.
          return result

      } else {
        // This locus can be incorporated in the current Contig without exceeding the region limit.

        curNumRegions += newRegions
        curIntervalOpt =
          curIntervalOpt match {
            // Extend the current interval, if possible.
            case Some((start, end)) if locus == end =>
              Some(start -> (locus + 1))

            // Otherwise, commit the current interval if it exists, and start a new one.
            case _ =>
              maybeAddInterval()
              Some(locus -> (locus + 1))
          }
        it.next()
      }
    }

    // We end up here if we ran out of loci on this contig before we reached the maximum allowed number of regions. In
    // this case, commit any in-progress interval, build the Contig, and return.
    maybeAddInterval()
    result
  }
}
