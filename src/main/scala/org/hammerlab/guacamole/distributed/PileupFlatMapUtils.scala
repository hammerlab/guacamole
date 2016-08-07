package org.hammerlab.guacamole.distributed

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.distributed.WindowFlatMapUtils.windowFlatMapWithState
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.reference.{ContigSequence, ReferenceGenome}
import org.hammerlab.guacamole.windowing.SlidingWindow

import scala.reflect.ClassTag

object PileupFlatMapUtils {
  /**
   * Helper function. Given optionally an existing Pileup, and a sliding read window return a new Pileup at the given
   * locus. If an existing Pileup is given as input, then the result will share elements with that Pileup for
   * efficiency.
   *
   *  If an existing Pileup is provided, then its locus must be <= the new locus.
   */
  private def initOrMovePileup(existing: Option[Pileup],
                               window: SlidingWindow[MappedRead],
                               referenceContigSequence: ContigSequence): Pileup = {
    assume(window.halfWindowSize == 0)
    existing match {
      case None => Pileup(
        window.currentRegions(), window.contigName, window.currentLocus, referenceContigSequence)
      case Some(pileup) => pileup.atGreaterLocus(window.currentLocus, window.newRegions.iterator)
    }
  }

  /**
   * Flatmap across loci, where at each locus the provided function is passed a Pileup instance.
   *
   * @param skipEmpty If true, the function will only be called at loci that have nonempty pileups, i.e. those
   *                  where at least one read overlaps. If false, then the function will be called at all the
   *                  specified loci. In cases where whole contigs may have no reads mapped (e.g. if running on
   *                  only a single chromosome, but loading loci from a sequence dictionary that includes the
   *                  entire genome), this is an important optimization.
   * @see the windowTaskFlatMapMultipleRDDs function for other argument descriptions
   *
   */
  def pileupFlatMap[T: ClassTag](reads: RDD[MappedRead],
                                 lociPartitions: LociPartitioning,
                                 skipEmpty: Boolean,
                                 function: Pileup => Iterator[T],
                                 reference: ReferenceGenome): RDD[T] = {
    windowFlatMapWithState(
      Vector(reads),
      lociPartitions,
      skipEmpty,
      halfWindowSize = 0,
      initialState = None,
      (maybePileup: Option[Pileup], windows: PerSample[SlidingWindow[MappedRead]]) => {
        assert(windows.length == 1)
        val pileup = initOrMovePileup(maybePileup, windows(0), reference.getContig(windows(0).contigName))
        (Some(pileup), function(pileup))
      }
    )
  }

  /**
   * Flatmap across loci on two RDDs of MappedReads. At each locus the provided function is passed two Pileup instances,
   * giving the pileup for the reads in each RDD at that locus.
   *
   * @param skipEmpty see [[pileupFlatMap]] for description.
   * @see the windowTaskFlatMapMultipleRDDs function for other argument descriptions.
   *
   */
  def pileupFlatMapTwoRDDs[T: ClassTag](reads1: RDD[MappedRead],
                                        reads2: RDD[MappedRead],
                                        lociPartitions: LociPartitioning,
                                        skipEmpty: Boolean,
                                        function: (Pileup, Pileup) => Iterator[T],
                                        reference: ReferenceGenome): RDD[T] = {
    windowFlatMapWithState(
      Vector(reads1, reads2),
      lociPartitions,
      skipEmpty,
      halfWindowSize = 0,
      initialState = None,
      function = (maybePileups: Option[(Pileup, Pileup)], windows: PerSample[SlidingWindow[MappedRead]]) => {
        assert(windows.length == 2)
        val contigSequence = reference.getContig(windows(0).contigName)
        val pileup1 = initOrMovePileup(maybePileups.map(_._1), windows(0), contigSequence)
        val pileup2 = initOrMovePileup(maybePileups.map(_._2), windows(1), contigSequence)
        (Some((pileup1, pileup2)), function(pileup1, pileup2))
      })
  }

  /**
   * Flatmap across loci and any number of RDDs of MappedReads.
   *
   * @see the windowTaskFlatMapMultipleRDDs function for other argument descriptions.
   */
  def pileupFlatMapMultipleRDDs[T: ClassTag](readsRDDs: PerSample[RDD[MappedRead]],
                                             lociPartitions: LociPartitioning,
                                             skipEmpty: Boolean,
                                             function: PerSample[Pileup] => Iterator[T],
                                             reference: ReferenceGenome): RDD[T] = {
    windowFlatMapWithState(
      readsRDDs,
      lociPartitions,
      skipEmpty,
      halfWindowSize = 0,
      initialState = None,
      function = (maybePileups: Option[PerSample[Pileup]], windows: PerSample[SlidingWindow[MappedRead]]) => {
        val advancedPileups =
          maybePileups match {

            case Some(existingPileups) =>
              for {
                (pileup, window) <- existingPileups.zip(windows)
              } yield
                initOrMovePileup(
                  Some(pileup),
                  window,
                  reference.getContig(windows(0).contigName)
                )

            case None =>
              windows.map(initOrMovePileup(None, _, reference.getContig(windows(0).contigName)))
        }
        (Some(advancedPileups), function(advancedPileups))
      })
  }
}
