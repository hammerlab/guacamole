package org.hammerlab.guacamole.readsets.rdd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulable, SparkContext}
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.PartitionIndex
import org.hammerlab.guacamole.loci.partitioning.{AllLociPartitionerArgs, LociPartitioning}
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.magic.accumulables.{HistogramParam, HashMap => MagicHashMap}
import org.hammerlab.magic.rdd.KeyPartitioner
import org.hammerlab.magic.rdd.SequenceFileSerializableRDD._
import org.hammerlab.magic.util.Stats

import scala.reflect.ClassTag

/**
 * Groups a {{LociPartitioning}} with an RDD[ReferenceRegion] that has already been partitioned according to the
 * partitioning.
 *
 * This means some regions will occur multiple times in the RDD (due to regions straddling partition boundaries, so it's
 * important not to confuse this with a regular RDD[ReferenceRegion].
 */
class PartitionedRegions[R <: ReferenceRegion: ClassTag](@transient val regions: RDD[R],
                                                         @transient val partitioning: LociPartitioning)
  extends Serializable {

  def sc: SparkContext = regions.sparkContext
  def hc: Configuration = sc.hadoopConfiguration

  lazy val partitionLociSets = partitioning.inverse.toArray.sortBy(_._1).map(_._2)
  lazy val lociSetsRDD = sc.parallelize(partitionLociSets, partitionLociSets.length).setName("lociSetsRDD")

  def save(fn: String, compressed: Boolean = true, overwrite: Boolean = false): this.type = {
    if (compressed)
      regions.saveCompressed(fn)
    else
      regions.saveSequenceFile(fn)
    this
  }
}

object PartitionedRegions {

  type IntHist = MagicHashMap[Int, Long]

  def IntHist(): IntHist = MagicHashMap[Int, Long]()

  def load[R <: ReferenceRegion: ClassTag](sc: SparkContext,
                                           fn: String,
                                           partitioning: LociPartitioning): PartitionedRegions[R] = {
    progress(s"Loading partitioned reads from $fn")
    val regions = sc.fromSequenceFile[R](fn, splittable = false)
    new PartitionedRegions(regions, partitioning)
  }

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            loci: LociSet,
                                            args: AllLociPartitionerArgs): PartitionedRegions[R] =
    apply(regions, loci, args, halfWindowSize = 0)

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            loci: LociSet,
                                            args: AllLociPartitionerArgs,
                                            halfWindowSize: Int): PartitionedRegions[R] = {

    val lociPartitioning = LociPartitioning(regions, loci, args, halfWindowSize)

    progress(
      s"Partitioned loci: ${lociPartitioning.numPartitions} partitions.",
      "Partition-size stats:",
      lociPartitioning.partitionSizeStats.toString(),
      "",
      "Contigs-spanned-per-partition stats:",
      lociPartitioning.partitionContigStats.toString()
    )

    apply(
      regions,
      lociPartitioning,
      halfWindowSize,
      args.partitionedReadsPathOpt,
      args.compressReadPartitions,
      !args.quiet
    )
  }

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            partitioning: LociPartitioning,
                                            halfWindowSize: Int,
                                            partitionedRegionsPathOpt: Option[String],
                                            compress: Boolean,
                                            printStats: Boolean): PartitionedRegions[R] = {

    val sc = regions.sparkContext

    partitionedRegionsPathOpt match {
      case Some(partitionedRegionsPath) =>

        val fs = FileSystem.get(sc.hadoopConfiguration)
        val path = new Path(partitionedRegionsPath)
        if (fs.exists(path))
          load(sc, partitionedRegionsPath, partitioning)
        else
          apply[R](regions, partitioning, halfWindowSize, partitionedRegionsPathOpt = None, compress, printStats)
            .save(partitionedRegionsPath, compressed = compress)

      case None =>

        val partitioningBroadcast = regions.sparkContext.broadcast(partitioning)

        val numPartitions = partitioning.numPartitions

        implicit val accumulableParam = new HistogramParam[Int, Long]

        // Histogram of the number of copies made of each region (i.e. when a region straddles loci-partition
        // boundaries.
        val regionCopiesHistogram: Accumulable[IntHist, Int] = sc.accumulable(IntHist(), "copies-per-region")

        // Histogram of the number of regions assigned to each partition.
        val partitionRegionsHistogram: Accumulable[IntHist, Int] = sc.accumulable(IntHist(), "regions-per-partition")

        val partitionedRegions =
          (for {
            r <- regions
            partitions = partitioningBroadcast.value.getAll(r, halfWindowSize)
            _ = (regionCopiesHistogram += partitions.size)
            partition <- partitions
          } yield {
            partitionRegionsHistogram += partition
            (partition, r.contig, r.start) -> r
          })
          .repartitionAndSortWithinPartitions(KeyPartitioner(numPartitions))
          .values
          .setName("partitioned-regions")

        if (printStats) {
          // Need to force materialization for the accumulator to have data… but that's reasonable because anything
          // downstream is presumably going to reuse this RDD.
          // TODO(ryan): worth adding a bypass here / pushing the printing of these statistics to later, for
          // applications that want to save this ~extra materialization. Worth benchmarking, in any case.
          val totalReadCopies = partitionedRegions.count

          val totalReads = regions.count

          // Sorted array of [number of read copies "K"] -> [number of reads that were copied "K" times].
          val regionCopies: Array[(Int, Long)] = regionCopiesHistogram.value.toArray.sortBy(_._1)

          val readsPlaced = regionCopies.filter(_._1 > 0).map(_._2).sum

          // Sorted array: [partition index "K"] -> [number of reads assigned to partition "K"].
          val regionsPerPartition: Array[(PartitionIndex, Long)] = partitionRegionsHistogram.value.toArray.sortBy(_._1)

          progress(
            s"Placed $readsPlaced of $totalReads (%.1f%%), %.1fx copies on avg; copies per read histogram:"
              .format(
                100.0 * readsPlaced / totalReads,
                totalReadCopies * 1.0 / readsPlaced
              ),
            Stats.fromHist(regionCopies).toString(),
            "",
            "Reads per partition stats:",
            Stats(regionsPerPartition.map(_._2)).toString()
          )

        }
        new PartitionedRegions(partitionedRegions, partitioning)
    }
  }

//  implicit def unwrapPartitionedRegions[R <: ReferenceRegion: ClassTag](pr: PartitionedRegions[R]): RDD[R] = pr.regions
}
