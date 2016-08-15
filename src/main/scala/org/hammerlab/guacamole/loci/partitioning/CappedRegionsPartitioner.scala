package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.readsets.rdd.CoverageRDD
import org.hammerlab.guacamole.reference.{ContigName, NumLoci, ReferenceRegion}
import org.hammerlab.magic.iterator.GroupRunsIterator
import org.hammerlab.magic.util.KeyOrdering
import org.kohsuke.args4j.{Option => Args4JOption}

import scala.reflect.ClassTag

trait CappedRegionsPartitionerArgs {
  @Args4JOption(
    name = "--max-reads-per-partition",
    usage = "Maximum number of reads to allow any one partition to have. Loci that have more depth than this will be dropped."
  )
  var maxReadsPerPartition: Int = 100000
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
class CappedRegionsPartitioner[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                                               halfWindowSize: Int,
                                                               maxRegionsPerPartition: Int,
                                                               printPartitioningStats: Boolean)
  extends LociPartitioner {

  def partition(loci: LociSet): LociPartitioning = {

    val coverageRDD = new CoverageRDD(regions)

    if (printPartitioningStats) {
      val (depthRunsRDD, validLoci, invalidLoci) =
        coverageRDD.validLociCounts(halfWindowSize, loci, maxRegionsPerPartition)

      val numDepthRuns = depthRunsRDD.count
      val numDepthRunsToTake = 1000
      val depthRuns =
        if (numDepthRuns <= numDepthRunsToTake)
          depthRunsRDD.collect()
        else
          depthRunsRDD.take(numDepthRunsToTake)

      val avgRunLength =
        (for {(_, num) <- depthRuns} yield num.toLong * num).sum.toDouble / validLoci

      val depthRunsByContig =
        depthRuns
          .groupBy(_._1._1)
          .mapValues(_.map {
            case ((_, valid), num) => num -> valid
          })
          .toArray
          .sorted(new KeyOrdering[ContigName, Array[(NumLoci, Boolean)]](ContigName.ordering))

      val overflowMsg =
        if (numDepthRuns > numDepthRunsToTake)
          s". First $numDepthRunsToTake runs:"
        else
          ":"

      def runsStr(runsIter: Iterator[(NumLoci, Boolean)]): String = {
        val runs = runsIter.toVector
        val rs =
          (for ((num, valid) <- runs) yield {
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

      progress(
        s"$validLoci (%.1f%%) loci with depth ≤$maxRegionsPerPartition, $invalidLoci other; $totalLoci total of ${loci.count} eligible)$overflowMsg"
          .format(100.0 * validLoci / totalLoci),
        (for {
          (contig, runs) <- depthRunsByContig
        } yield {

          val str =
            GroupRunsIterator[(NumLoci, Boolean)](runs, _._1 < avgRunLength)
              .map(runsStr)
              .mkString("\t\n")

          s"$contig:\t$str"
        }).mkString("\n")
      )
    }

    val lociSets =
      coverageRDD
        .makeCappedLociSets(
          halfWindowSize,
          loci,
          maxRegionsPerPartition
        )
        .collect()

    LociPartitioning(lociSets)
  }
}
