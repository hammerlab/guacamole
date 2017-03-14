package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.{ NumLoci, Region }
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.readsets.rdd.CoverageRDD
import org.hammerlab.iterator.GroupRunsIterator._
import org.kohsuke.args4j.{ Option ⇒ Args4JOption }

import scala.reflect.ClassTag

trait CappedRegionsPartitionerArgs
  extends HalfWindowConfig {

  @Args4JOption(
    name = "--max-reads-per-partition",
    usage = "Maximum number of reads to allow any one partition to have. Loci that have more depth than this will be dropped."
  )
  var maxReadsPerPartition: Int = 100000

  @Args4JOption(
    name = "--explode-coverage",
    usage =
      "When present, compute coverage-depths by \"exploding\" reads into per-locus, depth-1 tuples before (map-side) reduction. Otherwise / By default, coverage depth is accumulated by a traversal of (assumedly start-pos-sorted) reads, emitting (locus,depth) pairs that have already \"reduced\" contributions to each locus within each partition"
  )
  var explodeCoverage: Boolean = false

  @Args4JOption(
    name = "--trim-ranges",
    usage =
      "When present, store which loci in each partition are actually covered, as opposed to one blanket range (per partition per contig) spanning from the smallest to largest loci assigned to each partition. This can result in more informative statistics being collected/printed about the computed loci-partitioning, but it can also result in a significant performance hit if many discontiguous ranges or coverage must be stored / collected to the driver and processed."
  )
  var trimRanges: Boolean = false
}

/**
 * Loci-partitioner that guarantees at most @maxRegionsPerPartition regions per partition.
 *
 *   - Loci with more than that many regions will be dropped.
 *   - Optionally prints summary stats about measured coverage.
 *
 * @param regions regions to partition loci based on.
 * @param halfWindowSize consider regions to overlap loci that their ends are within this many base-pairs of.
 * @param maxRegionsPerPartition cap partitions at this many regions.
 * @param printPartitioningStats print some statistics about the computed partitioning; adds several Spark stages so should be
 *                   skipped for performance-critical runs.
 */
class CappedRegionsPartitioner[R <: Region: ClassTag](regions: RDD[R],
                                                      halfWindowSize: Int,
                                                      maxRegionsPerPartition: Int,
                                                      printPartitioningStats: Boolean,
                                                      explodeCoverage: Boolean,
                                                      trimRanges: Boolean)
  extends LociPartitioner {

  def partition(loci: LociSet): LociPartitioning = {

    val coverageRDD = new CoverageRDD(regions)

    val lociBroadcast = regions.sparkContext.broadcast(loci)

    if (printPartitioningStats) {
      printStats(coverageRDD, lociBroadcast)
    }

    val lociSets =
      coverageRDD
        .makeCappedLociSets(
          halfWindowSize,
          loci,
          maxRegionsPerPartition,
          explodeCoverage,
          trimRanges
        )
        .collect()

    LociPartitioning(lociSets)
  }

  private def printStats(coverageRDD: CoverageRDD[R], lociBroadcast: Broadcast[LociSet]): Unit = {

    val (depthRunsRDD, validLoci, invalidLoci) =
      coverageRDD.validLociCounts(halfWindowSize, lociBroadcast, maxRegionsPerPartition)

    val numDepthRuns = depthRunsRDD.count
    val numDepthRunsToTake = 1000
    val depthRuns =
      if (numDepthRuns <= numDepthRunsToTake)
        depthRunsRDD.collect()
      else
        depthRunsRDD.take(numDepthRunsToTake)

    val avgRunLength =
      (for { (_, num) ← depthRuns } yield num.toLong * num).sum.toDouble / validLoci

    val depthRunsByContig =
      depthRuns
        .groupBy(_._1._1)
        .mapValues(_.map {
          case ((_, valid), num) ⇒ num → valid
        })
        .toArray
        .sortBy(_._1)

    val overflowMsg =
      if (numDepthRuns > numDepthRunsToTake)
        s". First $numDepthRunsToTake runs:"
      else
        ":"

    def runsStr(runsIter: Iterator[(NumLoci, Boolean)]): String = {
      val runs = runsIter.toVector
      val rs =
        (for ((num, valid) ← runs) yield {
          s"$num${if (valid) "↓" else "↑"}"
        }).mkString(" ")
      if (runs.length == 1)
        s"$rs"
      else {
        val total = runs.map(_._1.toLong).sum
        s"${runs.length} runs, $total loci (avg %.1f): $rs".format(total.toDouble / runs.length)
      }
    }

    val totalLoci = validLoci + invalidLoci

    val loci = lociBroadcast.value

    progress(
      s"$validLoci (%.1f%%) loci with depth ≤$maxRegionsPerPartition, $invalidLoci other; $totalLoci total of ${loci.count} eligible)$overflowMsg"
      .format(100.0 * validLoci / totalLoci),
      (for {
        (contig, runs) ← depthRunsByContig
      } yield {

        val str =
          runs
            .iterator
            .buffered
            .groupBy(x ⇒ x._1 < avgRunLength)
            .map(runsStr)
            .mkString("\t\n")

        s"$contig:\t$str"
      }).mkString("\n")
    )
  }
}
