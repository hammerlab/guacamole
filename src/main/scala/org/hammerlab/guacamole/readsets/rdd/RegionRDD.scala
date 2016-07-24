package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.NumPartitions
import org.hammerlab.guacamole.loci.set.{LociSet, TakeLociIterator}
import org.hammerlab.guacamole.readsets.ContigLengths
import org.hammerlab.guacamole.readsets.iterator.{ContigCoverageIterator, ContigsIterator}
import org.hammerlab.guacamole.reference.{ContigName, NumLoci, Position, ReferenceRegion}
import org.hammerlab.magic.rdd.RDDStats._
import org.hammerlab.magic.rdd.RunLengthRDD._
import org.hammerlab.magic.util.Stats

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Augment an RDD[ReferenceRegion] with some useful methods for e.g. computing coverage depth.
 */
class RegionRDD[R <: ReferenceRegion: ClassTag](@transient rdd: RDD[R])
  extends Serializable {

  @transient val sc = rdd.sparkContext

  /**
   * Convenience stats about the partitions of the wrapped RDD[R].
   */
  @transient lazy val (
    numEmptyPartitions: Int,
    numPartitionsSpanningContigs: Int,
    partitionSpans: ArrayBuffer[Long],
    spanStats: Stats[NumLoci, NumPartitions],
    nonEmptySpanStats: Stats[NumLoci, NumPartitions]
  ) = {
    val partitionBounds = rdd.partitionBounds
    var numEmpty = 0
    var numCrossingContigs = 0
    val spans = ArrayBuffer[NumLoci]()
    val nonEmptySpans = ArrayBuffer[NumLoci]()

    partitionBounds.foreach {
      case None =>
        numEmpty += 1
        spans += 0L
      case Some((first, last)) if first.contigName == last.contigName =>
        val span = last.start - first.start
        nonEmptySpans += span
        spans += span
      case _ =>
        numCrossingContigs += 1
        None
    }

    (numEmpty, numCrossingContigs, spans, Stats(spans), Stats(nonEmptySpans))
  }

  /**
   * Compute a PositionCoverage for every position in @loci, allowing a half-window of @halfWindowSize. Caches results.
   */
  @transient val coverages_ = mutable.Map[(Int, LociSet), RDD[PositionCoverage]]()
  def coverage(halfWindowSize: Int, loci: LociSet): RDD[PositionCoverage] = coverage(halfWindowSize, sc.broadcast(loci))
  def coverage(halfWindowSize: Int, lociBroadcast: Broadcast[LociSet]): RDD[PositionCoverage] =
    coverages_.getOrElseUpdate(
      (halfWindowSize, lociBroadcast.value),
      {
        rdd
          .mapPartitions(it => {
            val loci = lociBroadcast.value
            for {
              (contigRegionsIterator, contigLociIterator) <- new ContigsIterator(it.buffered, loci)
              contig = contigRegionsIterator.contigName
              coverage <- ContigCoverageIterator(halfWindowSize, contigRegionsIterator, contigLociIterator)
            } yield
              coverage
          })
          .reduceByKey(_ + _)
          .sortByKey()
      }
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
  coverage(halfWindowSize, sc.broadcast(loci)).mapPartitionsWithIndex((idx, it) =>
    new TakeLociIterator(it.buffered, maxRegionsPerPartition)
  )

  /**
   * Compute the depth at each locus in @rdd, then group loci into runs that are uniformly above or below @depthCutoff.
   *
   * Useful for getting a sense of which parts of the genome have exceedingly high coverage.
   */
  def partitionDepths(halfWindowSize: Int,
                      loci: LociSet,
                      depthCutoff: Int): RDD[((ContigName, Boolean), Int)] = {
    (for {
      (Position(contig, _), Coverage(depth, _, _)) <- coverage(halfWindowSize, loci)
    } yield
      contig -> (depth <= depthCutoff)
      ).runLengthEncode
  }

  /**
   * Alternative implementation of this.coverage; will generally perform significantly worse on sorted inputs. Useful as
   * a sanity check.
   */
  def shuffleCoverage(halfWindowSize: Int, contigLengthsBroadcast: Broadcast[ContigLengths]): RDD[PositionCoverage] = {
    rdd
    .flatMap(r => {
      val c = r.contigName
      val length = contigLengthsBroadcast.value(c)

      val lowerBound = math.max(0, r.start - halfWindowSize)
      val upperBound = math.min(length, r.end + halfWindowSize)

      val outs = ArrayBuffer[(Position, Coverage)]()
      for {
        l <- lowerBound until upperBound
      } {
        outs += Position(c, l) -> Coverage(depth = 1)
      }

      outs += Position(c, lowerBound) -> Coverage(starts = 1)
      outs += Position(c, upperBound) -> Coverage(ends = 1)

      outs.iterator
    })
    .reduceByKey(_ + _)
    .sortByKey()
  }

  /**
   * Wrappers around this.coverage that map genomic loci to their depth and numbers of read starts and ends, sorted in
   * descending order of the latter.
   */
  @transient val depths_ = mutable.Map[(Int, LociSet), RDD[(Int, Position)]]()
  def depths(halfWindowSize: Int, loci: LociSet): RDD[(Int, Position)] =
    depths_.getOrElseUpdate(
      halfWindowSize -> loci,
      coverage(halfWindowSize, loci).map(t => t._2.depth -> t._1).sortByKey(ascending = false)
    )

  @transient val starts_ = mutable.Map[(Int, LociSet), RDD[(Int, Position)]]()
  def starts(halfWindowSize: Int, loci: LociSet): RDD[(Int, Position)] =
    starts_.getOrElseUpdate(
      halfWindowSize -> loci,
      coverage(halfWindowSize, loci).map(t => t._2.starts -> t._1).sortByKey(ascending = false)
    )

  @transient val ends_ = mutable.Map[(Int, LociSet), RDD[(Int, Position)]]()
  def ends(halfWindowSize: Int, loci: LociSet): RDD[(Int, Position)] =
    ends_.getOrElseUpdate(
      halfWindowSize -> loci,
      coverage(halfWindowSize, loci).map(t => t._2.ends -> t._1).sortByKey(ascending = false)
    )
}

object RegionRDD {
  /**
   * Since RegionRDDs cache coverage info they compute, it's potentially beneficial to cache each RDD's corresponding
   * RegionRDD.
   *
   * We include the SparkContext in the lookup to avoid collisions between RDDs with a given ID from two different
   * test suites, each of which commonly will have its own SparkContext.
   */
  private val rddMap = mutable.Map[(SparkContext, Int), RegionRDD[_]]()
  implicit def rddToRegionRDD[R <: ReferenceRegion: ClassTag](rdd: RDD[R]): RegionRDD[R] =
    rddMap.getOrElseUpdate(
      rdd.sparkContext -> rdd.id,
      new RegionRDD[R](rdd)
    ).asInstanceOf[RegionRDD[R]]

  /**
   * Given an RDD output by {{RegionRDD.partitionDepths}}, count the number of loci below (resp. above) the depth
   * cutoff.
   */
  def validLociCounts(depthRuns: RDD[((ContigName, Boolean), Int)]): (NumLoci, NumLoci) = {
    val map =
      (for {
        ((_, validDepth), numLoci) <- depthRuns
      } yield
        validDepth -> numLoci.toLong
      )
      .reduceByKey(_ + _)
      .collectAsMap

    (map.getOrElse(true, 0), map.getOrElse(false, 0))
  }
}
