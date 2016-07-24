package org.hammerlab.guacamole.readsets.rdd

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.distributed.{HashMapAccumulatorParam, KeyPartitioner, TaskPosition}
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.logging.DelayedMessages
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.reference.ReferenceRegion

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.reflect.ClassTag

object PartitionedRegions {
  def apply[R <: ReferenceRegion: ClassTag](regionRDDs: PerSample[RDD[R]],
                                            lociPartitionsBoxed: Broadcast[LociPartitioning],
                                            halfWindowSize: Int): RDD[(Int, R)] = {

    val sc = regionRDDs(0).sparkContext

    val lociPartitions = lociPartitionsBoxed.value

    progress(s"Partitioning reads according to loci partitioning:\n$lociPartitions")

    val numTasks = lociPartitions.inverse.map(_._1).max + 1

    // Counters
    val totalRegions = sc.accumulator(0L)
    val relevantRegions = sc.accumulator(0L)
    val expandedRegions = sc.accumulator(0L)

    DelayedMessages.default.say { () =>
      "Region counts: filtered %,d total regions to %,d relevant regions, expanded for overlaps by %,.2f%% to %,d".format(
        totalRegions.value,
        relevantRegions.value,
        (expandedRegions.value - relevantRegions.value) * 100.0 / relevantRegions.value,
        expandedRegions.value)
    }

    // Expand regions into (task, region) pairs for each region RDD.
    val taskNumberRegionPairsRDDs: PerSample[RDD[(TaskPosition, R)]] =
      regionRDDs.map(
        _.flatMap(region => {
          val singleContig = lociPartitionsBoxed.value.onContig(region.contigName)
          val partitionsForRegion = singleContig.getAll(region.start - halfWindowSize, region.end + halfWindowSize)

          // Update counters
          totalRegions += 1
          if (partitionsForRegion.nonEmpty) relevantRegions += 1
          expandedRegions += partitionsForRegion.size

          // Return this region, duplicated for each task it is assigned to.
          partitionsForRegion.map(task => TaskPosition(task, region.contigName, region.start) -> region)
        })
      )

    // Run the task on each partition. Keep track of the number of regions assigned to each task in an accumulator, so
    // we can print out a summary of the skew.
    val regionsByTask = sc.accumulator(MutableHashMap.empty[String, Long])(new HashMapAccumulatorParam)

    DelayedMessages.default.say {
      () => {
        val stats = new DescriptiveStatistics()
        regionsByTask.value.valuesIterator.foreach(stats.addValue(_))
        "Regions per task: min=%,.0f 25%%=%,.0f median=%,.0f (mean=%,.0f) 75%%=%,.0f max=%,.0f. Max is %,.2f%% more than mean.".format(
          stats.getMin,
          stats.getPercentile(25),
          stats.getPercentile(50),
          stats.getMean,
          stats.getPercentile(75),
          stats.getMax,
          (stats.getMax - stats.getMean) * 100.0 / stats.getMean
        )
      }
    }

    // Build an RDD of (read set num, read), take union of this over all RDDs, and partition by task.
    sc
      .union(
        for {
          (keyedRegionsRDD, sampleId) <- taskNumberRegionPairsRDDs.zipWithIndex
        } yield {
          for {
            (taskPosition, read) <- keyedRegionsRDD
          } yield
            taskPosition -> (sampleId, read)
        }
      )
      .repartitionAndSortWithinPartitions(KeyPartitioner(numTasks))
      .values
  }
}
