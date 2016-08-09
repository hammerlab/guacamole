package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.set.{LociSet, TakeLociIterator}
import org.hammerlab.guacamole.readsets.ContigLengths
import org.hammerlab.guacamole.readsets.iterator.{ContigCoverageIterator, ContigsIterator}
import org.hammerlab.guacamole.reference.{ContigName, NumLoci, Position, ReferenceRegion}
import org.hammerlab.magic.rdd.RunLengthRDD._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Augment an RDD[ReferenceRegion] with some useful methods for e.g. computing coverage depth.
 *
 * @param requireRegionsGroupedByContig when true, require regions to be grouped by contig within each partition; this
 *                                      is necessary for some of our iteration to work efficiently.
 */
class CoverageRDD[R <: ReferenceRegion: ClassTag](@transient rdd: RDD[R], requireRegionsGroupedByContig: Boolean)
  extends Serializable {

  @transient val sc = rdd.sparkContext

  /**
   * Compute a PositionCoverage for every position in @loci, allowing a half-window of @halfWindowSize.
   */
  def coverage(halfWindowSize: Int, loci: LociSet): RDD[(Position, Coverage)] =
    coverage(halfWindowSize, sc.broadcast(loci))

  def coverage(halfWindowSize: Int,
               lociBroadcast: Broadcast[LociSet]): RDD[(Position, Coverage)] =
    rdd
      .mapPartitions(it =>
        for {
          (contigName, contigRegions) <- ContigsIterator(it.buffered, requireRegionsGroupedByContig)
          contigCoverages = ContigCoverageIterator(halfWindowSize, contigRegions)
          lociContig = lociBroadcast.value.onContig(contigName).iterator
          (locus, coverage) <- contigCoverages.intersect(lociContig)
        } yield
          Position(contigName, locus) -> coverage
      )
    .reduceByKey(_ + _)
    .sortByKey()

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
        new TakeLociIterator(it.buffered, maxRegionsPerPartition, requireRegionsGroupedByContig)
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
                      contigLengthsBroadcast: Broadcast[ContigLengths]): RDD[(Position, Coverage)] = {
    rdd
      .flatMap(r => {
        val c = r.contigName
        val length = contigLengthsBroadcast.value(c)

        val lowerBound = math.max(0, r.start - halfWindowSize)
        val upperBound = math.min(length, r.end + halfWindowSize)

        val outs = ArrayBuffer[(Position, Coverage)]()

        outs += Position(c, lowerBound) -> Coverage(starts = 1)

        for {
          l <- lowerBound until upperBound
        } {
          outs += Position(c, l) -> Coverage(depth = 1)
        }

        outs.iterator
      })
      .reduceByKey(_ + _)
      .sortByKey()
  }
}
