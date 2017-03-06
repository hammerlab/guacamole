package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.genomics.loci.iterator.LociIterator
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.Position.totalOrdering
import org.hammerlab.genomics.reference.{ ContigName, ContigsIterator, Interval, NumLoci, Position, Region }
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.set.TakeLociIterator
import org.hammerlab.guacamole.readsets.iterator.ContigCoverageIterator
import org.hammerlab.magic.rdd.RunLengthRDD._

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Augment an [[RDD[Region]]] with methods for computing coverage depth.
 */
class CoverageRDD[R <: Region: ClassTag](rdd: RDD[R])
  extends Serializable {

  @transient val sc = rdd.sparkContext

  // Cache of [[coverage]]s computed below.
  private val _coveragesCache = mutable.HashMap[(Int, LociSet), RDD[(Position, Coverage)]]()

  /**
   * Compute a PositionCoverage for every position in @loci, allowing a half-window of @halfWindowSize.
   *
   * @param halfWindowSize count bases as contributing coverage to a window that extends this many loci in either
   *                       direction.
   * @param lociBroadcast Spark Broadcast of a set of loci to compute depths for.
   * @param explode If true, emit (locus, 1) tuples for every region-base before letting Spark do map-side, then
   *                reduce-side, reductions. Otherwise, traverse regions, emitting (locus, depth) tuples for all regions
   *                in a partition that overlap a current locus, effectively folding the map-side-reduction into
   *                application code, as an optimization.
   * @return RDD of (Position, Coverage) tuples giving the total coverage, and number of region-starts, at each
   *         genomic position in `lociBroadcast`.
   */
  def coverage(halfWindowSize: Int,
               lociBroadcast: Broadcast[LociSet],
               explode: Boolean = false): RDD[(Position, Coverage)] =
    _coveragesCache
      .getOrElseUpdate(
        (halfWindowSize, lociBroadcast.value),
        if (explode)
          explodedCoverage(halfWindowSize, lociBroadcast)
        else
          traversalCoverage(halfWindowSize, lociBroadcast)
      )

  /**
   * Break the input @loci into smaller LociSets such that the number of regions (with a @halfWindowSize grace-window)
   * overlapping each set is ≤ @maxRegionsPerPartition.
   *
   * First obtains the "coverage" RDD, then takes regions greedily, meaning the end of each partition of the coverage-RDD
   * will tend to have a "remainder" LociSet that has ≈half the maximum regions per partition.
   */
  def makeCappedLociSets(halfWindowSize: Int,
                         loci: LociSet,
                         maxRegionsPerPartition: Int,
                         explode: Boolean,
                         trimRanges: Boolean): RDD[LociSet] =
    coverage(
      halfWindowSize,
      sc.broadcast(loci),
      explode
    )
    .mapPartitions(
      it ⇒
        new TakeLociIterator(
          it.buffered,
          maxRegionsPerPartition,
          trimRanges
        )
    )

  private implicit val contigNameBoolOrdering = Ordering.by[(ContigName, Boolean), ContigName](_._1)

  private implicit val positionOrdering = totalOrdering

  /**
   * Compute the depth at each locus in @rdd, then group loci into runs that are uniformly below (true) or above (false)
   * `depthCutoff`.
   *
   * Useful for getting a sense of which parts of the genome have exceedingly high coverage.
   *
   * @param halfWindowSize see [[coverage]].
   * @param lociBroadcast see [[coverage]].
   * @param depthCutoff separate runs of loci that are uniformly below (or equal to) vs. above (>) this cutoff.
   * @return [[RDD]] whose elements have:
   *        - a key consisting of a contig name and a boolean indicating whether loci represented by this element have
   *          coverage depth ≤ `depthCutoff`, and
   *        - a value indicating the length of a run of loci with depth above or below `depthCutoff`, as described
   *          above.
   */
  def partitionDepths(halfWindowSize: Int,
                      lociBroadcast: Broadcast[LociSet],
                      depthCutoff: Int): RDD[((ContigName, Boolean), NumLoci)] =
    (
      for {
        (Position(contig, _), Coverage(depth, _)) ← coverage(halfWindowSize, lociBroadcast)
      } yield
        contig → (depth <= depthCutoff)
    )
    .runLengthEncode
    .mapValues(NumLoci(_))

  /**
   * Compute the coverage-depth at each locus, then aggregate loci into runs that are all above or below `depthCutoff`.
   *
   * @return tuple containing an [[RDD]] returned by [[partitionDepths]] as well as the total numbers of loci with depth
   *         below (or equal to) `depthCutoff` (resp. above `depthCutoff`).
   */
  def validLociCounts(halfWindowSize: Int,
                      lociBroadcast: Broadcast[LociSet],
                      depthCutoff: Int): (RDD[((ContigName, Boolean), NumLoci)], NumLoci, NumLoci) = {
    val depthRuns = partitionDepths(halfWindowSize, lociBroadcast, depthCutoff)
    val map =
      (for {
        ((_, validDepth), numLoci) ← depthRuns
      } yield
        validDepth → numLoci
      )
      .reduceByKey(_ + _)
      .collectAsMap

    (
      depthRuns,
      map.getOrElse(true, NumLoci(0)),
      map.getOrElse(false, NumLoci(0))
    )
  }

  private[rdd] def traversalCoverage(halfWindowSize: Int,
                                     lociBroadcast: Broadcast[LociSet]): RDD[(Position, Coverage)] =
    rdd
      .mapPartitions(
        it ⇒
          for {

            // For each contig, and only the regions that lie on it…
            (contigName, contigRegions) ← ContigsIterator(it.buffered)

            // Iterator over loci overlapped, each with a Coverage recording coverage-depth data.
            contigCoverages = ContigCoverageIterator(halfWindowSize, contigRegions)

            // Iterator of eligible loci on this contig.
            lociContig = lociBroadcast.value(contigName).iterator

            // Filter (locus, coverage) iterator by eligible loci for this contig.
            (locus, coverage) ← contigCoverages.intersect(lociContig)

          } yield
            Position(contigName, locus) → coverage
      )
      .reduceByKey(_ + _)
      .sortByKey()

  private def coveragesForRegion(region: Region,
                                 halfWindowSize: Int,
                                 loci: LociSet): Iterator[(Position, Coverage)] = {

    val Region(contigName, start, end) = region

    // Compute the bounds of loci that this region should contribute 1 unit of coverage-depth to.
    val lowerBound = start - halfWindowSize
    val upperBound = end + halfWindowSize

    // Iterator over loci spanned by the current region, including the half-window buffer on each end.
    val regionLociIterator = new LociIterator(Iterator(Interval(lowerBound, upperBound)).buffered)

    // Eligible loci on this region's contig.
    val lociContig = loci(contigName).iterator

    // Intersect the eligible loci with the region's loci.
    val lociIterator = lociContig.intersect(regionLociIterator)

    var regionStart = true

    for {
      // Each resulting locus is covered by the region (±halfWindowSize), and is part of the valid LociSet.
      locus ← lociIterator

      position = Position(contigName, locus)

      // All covered loci should get one unit of coverage-depth recorded due to this region.
      depthCoverage = Coverage(depth = 1)

      // The first locus should also record one unit of "region-start depth", which is also recorded by `Coverage`
      // objects, and region downstream by code computing the number of regions straddling partition boundaries.
      coverages =
        if (regionStart) {
          regionStart = false
          List(Coverage(starts = 1), depthCoverage)
        } else
          List(depthCoverage)

      // For each of the (1 or 2) Coverages above…
      coverage ← coverages

    } yield
      // Emit the Coverage, keyed by the current genomic position.
      position → coverage
  }

  /**
   * Alternative implementation of this.coverage; will generally perform significantly worse on sorted inputs. Useful as
   * a sanity check.
   */
  private[rdd] def explodedCoverage(halfWindowSize: Int,
                                    lociBroadcast: Broadcast[LociSet]): RDD[(Position, Coverage)] =
    rdd
      .flatMap(coveragesForRegion(_, halfWindowSize, lociBroadcast.value))
      .reduceByKey(_ + _)  // sum all Coverages for each Position
      .sortByKey()  // sort by Position
}

object CoverageRDD {
  implicit def toCoverageRDD[R <: Region: ClassTag](rdd: RDD[R]): CoverageRDD[R] = new CoverageRDD(rdd)
}
