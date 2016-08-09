package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.set.{LociSet, TakeLociIterator}
import org.hammerlab.guacamole.readsets.ContigLengths
import org.hammerlab.guacamole.readsets.iterator.{ContigCoverageIterator, ContigsIterator}
import org.hammerlab.guacamole.reference.{ContigName, NumLoci, Position, ReferenceRegion}
import org.hammerlab.magic.rdd.RunLengthRDD._

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Augment an [[RDD[ReferenceRegion]]] with methods for computing coverage depth, .
 */
class CoverageRDD[R <: ReferenceRegion: ClassTag](@transient rdd: RDD[R])
  extends Serializable {

  @transient val sc = rdd.sparkContext

  /**
   * Compute a PositionCoverage for every position in @loci, allowing a half-window of @halfWindowSize.
   */
  def coverage(halfWindowSize: Int, loci: LociSet): RDD[(Position, Coverage)] =
    coverage(halfWindowSize, sc.broadcast(loci))

  private val _coveragesCache = mutable.HashMap[(Int, LociSet), RDD[(Position, Coverage)]]()
  def coverage(halfWindowSize: Int,
               lociBroadcast: Broadcast[LociSet]): RDD[(Position, Coverage)] =
    _coveragesCache.getOrElseUpdate(
      (halfWindowSize, lociBroadcast.value),
      rdd
        .mapPartitions(it =>
          for {

            // For each contig, and only the regions that lie on it…
            (contigName, contigRegions) <- ContigsIterator(it.buffered)

            // Iterator over loci overlapped, each with a Coverage recording coverage-depth data.
            contigCoverages = ContigCoverageIterator(halfWindowSize, contigRegions)

            // Iterator of eligible loci on this contig.
            lociContig = lociBroadcast.value.onContig(contigName).iterator

            // Filter (locus, coverage) iterator by eligible loci for this contig.
            (locus, coverage) <- contigCoverages.intersect(lociContig)

          } yield
            Position(contigName, locus) -> coverage
        )
        .reduceByKey(_ + _)
        .sortByKey()
    )

  /**
   * Break the input @loci into smaller LociSets such that the number of reads (with a @halfWindowSize grace-window)
   * overlapping each set is ≤ @maxRegionsPerPartition.
   *
   * First obtains the "coverage" RDD, then takes reads greedily, meaning the end of each partition of the coverage-RDD
   * will tend to have a "remainder" LociSet that has ≈half the maximum regions per partition.
   */
  def makeCappedLociSets(halfWindowSize: Int,
                         loci: LociSet,
                         maxRegionsPerPartition: Int): RDD[LociSet] =
    coverage(
      halfWindowSize,
      sc.broadcast(loci)
    )
    .mapPartitionsWithIndex(
      (idx, it) =>
        new TakeLociIterator(it.buffered, maxRegionsPerPartition)
    )

  /**
   * Compute the depth at each locus in @rdd, then group loci into runs that are uniformly below (true) or above (false)
   * `depthCutoff`.
   *
   * Useful for getting a sense of which parts of the genome have exceedingly high coverage.
   *
   * @param halfWindowSize see [[coverage]].
   * @param loci see [[coverage]].
   * @param depthCutoff separate runs of loci that are uniformly below (or equal to) vs. above (>) this cutoff.
   * @return [[RDD]] whose elements have:
   *        - a key consisting of a contig name and a boolean indicating whether loci represented by this element have
   *          coverage depth ≤ `depthCutoff`, and
   *        - a value indicating the length of a run of loci with depth above or below `depthCutoff`, as described
   *          above.
   */
  def partitionDepths(halfWindowSize: Int,
                      loci: LociSet,
                      depthCutoff: Int): RDD[((ContigName, Boolean), Long)] = {
    (for {
      (Position(contig, _), Coverage(depth, _)) <- coverage(halfWindowSize, loci)
    } yield
      contig -> (depth <= depthCutoff)
    ).runLengthEncode
  }

  /**
   * Compute the coverage-depth at each locus, then aggregate loci into runs that are all above or below `depthCutoff`.
   *
   * @return tuple containing an [[RDD]] returned by [[partitionDepths]] as well as the total numbers of loci with depth
   *         below (or equal to) `depthCutoff` (resp. above `depthCutoff`).
   */
  def validLociCounts(halfWindowSize: Int,
                      loci: LociSet,
                      depthCutoff: Int): (RDD[((ContigName, Boolean), NumLoci)], NumLoci, NumLoci) = {
    val depthRuns = partitionDepths(halfWindowSize, loci, depthCutoff)
    val map =
      (for {
        ((_, validDepth), numLoci) <- depthRuns
      } yield
        validDepth -> numLoci.toLong
        )
      .reduceByKey(_ + _)
      .collectAsMap

    (depthRuns, map.getOrElse(true, 0), map.getOrElse(false, 0))
  }

  /**
   * Alternative implementation of this.coverage; will generally perform significantly worse on sorted inputs. Useful as
   * a sanity check.
   */
  def shuffleCoverage(halfWindowSize: Int,
                      contigLengthsBroadcast: Broadcast[ContigLengths]): RDD[(Position, Coverage)] =
    (for {

      // For each region…
      ReferenceRegion(contigName, start, end) <- rdd

      // Compute the bounds of loci that this region should contribute 1 unit of coverage-depth to.
      contigLength = contigLengthsBroadcast.value(contigName)
      lowerBound = math.max(0, start - halfWindowSize)
      upperBound = math.min(contigLength, end + halfWindowSize)

      // For each such locus…
      locus <- lowerBound until upperBound

      position = Position(contigName, locus)

      // All covered loci should get one unit of coverage-depth recorded due to this region.
      depthCoverage = Coverage(depth = 1)

      // The first locus should also record one unit of "region-start depth", which is also recorded by `Coverage`
      // objects, and read downstream by code computing the number of regions straddling partition boundaries.
      coverages =
        if (locus == lowerBound)
          List(Coverage(starts = 1), depthCoverage)
        else
          List(depthCoverage)

      // For each of the (1 or 2) Coverages above…
      coverage <- coverages

    } yield
      // Emit the Coverage, keyed by the current genomic position.
      position -> coverage
    )
    .reduceByKey(_ + _)  // sum all Coverages for each Position
    .sortByKey()  // sort by Position
}
