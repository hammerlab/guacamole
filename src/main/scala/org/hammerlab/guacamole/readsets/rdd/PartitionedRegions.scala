package org.hammerlab.guacamole.readsets.rdd

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulable, SparkContext}
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.Region
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.magic.accumulables.{HistogramParam, HashMap => MagicHashMap}
import org.hammerlab.magic.rdd.serde.SequenceFileSerializableRDD._
import org.hammerlab.spark.PartitionIndex
import org.hammerlab.spark.util.KeyPartitioner
import org.hammerlab.stats.Stats

import scala.reflect.ClassTag

/**
 * Groups a [[LociPartitioning]] with an [[RDD[Region]]] that has already been partitioned according to that
 * partitioning.
 *
 * This means some regions will occur multiple times in the RDD (due to regions straddling partition boundaries), so
 * it's important not to confuse this with a regular [[RDD[Region]]].
 *
 * The main API exposed here is [[mapPartitions]], which lets the caller apply a function to a [[org.hammerlab.genomics.loci.set.LociSet]] as well as
 * all region copies that overlap those loci.
 *
 * Note: the containing [[PartitionedRegions]] gets picked up by the closure-cleaner and serialized when
 * [[mapPartitions]] is called.
 */
class PartitionedRegions[R <: Region: ClassTag] private(regions: RDD[R],
                                                        partitioning: LociPartitioning)
  extends Serializable {

  @transient lazy val lociSetsRDD = partitioning.lociSetsRDD(sc)

  assert(
    regions.getNumPartitions == lociSetsRDD.getNumPartitions,
    s"reads partitions: ${regions.getNumPartitions}, loci partitions: ${lociSetsRDD.getNumPartitions}"
  )

  def sc: SparkContext = regions.sparkContext

  // Accumulator that is used in `mapPartitions` below.
  val numLoci = sc.accumulator(0L, "numLoci")

  /**
   * For each partition, apply a function to the set of loci assigned to that partition as well as all regions that
   * overlap those loci (possibly with a grace-window baked in upstream; see [[PartitionedRegions]] constructors below).
   *
   * @param f function that operates on the regions and loci corresponding to a partition, emiting an output iterator of
   *          arbitrary type [[V]].
   * @return [[RDD[V]]], with partitions comprised of the [[Iterator[V]]]'s returned by application of `f` to each
   *        partition.
   */
  def mapPartitions[V: ClassTag](f: (Iterator[R], LociSet) => Iterator[V]): RDD[V] =
    regions
      .zipPartitions(
        lociSetsRDD,
        preservesPartitioning = true
      )(
        (regionsIter, lociIter) => {
          val loci = lociIter.next()
          if (lociIter.hasNext) {
            throw new Exception(s"Expected 1 LociSet, found ${1 + lociIter.size}.\n$loci")
          }

          numLoci += loci.count

          f(regionsIter, loci)
        }
      )

  /**
   * Write the partitioned regions RDD to a file.
   */
  def save(filename: String, compressed: Boolean = true, overwrite: Boolean = false): this.type = {
    if (compressed)
      regions.saveCompressed(filename)
    else
      regions.saveSequenceFile(filename)

    this
  }
}

object PartitionedRegions {

  type IntHist = MagicHashMap[Int, Long]

  def IntHist(): IntHist = MagicHashMap[Int, Long]()

  /**
   * Load an [[RDD]] of partitioned regions from a file.
   */
  def load[R <: Region: ClassTag](sc: SparkContext,
                                  filename: String,
                                  partitioning: LociPartitioning): PartitionedRegions[R] = {
    progress(s"Loading partitioned reads from $filename")
    val regions = sc.fromSequenceFile[R](filename, splittable = false)
    new PartitionedRegions(regions, partitioning)
  }

  /**
   * Main [[PartitionedRegions]] constructor: given some regions and loci, assign the loci to Spark partitions, and then
   * partition the regions according to which partitions' loci they overlap.
   *
   * Computes a [[LociPartitioning]] and delegates to the other constructor below.
   *
   * @param regions RDDs of regions to partition.
   * @param loci Genomic loci to operate on; these will be split among Spark partitions and coupled with all regions
   *             from `regionRDDs` that overlap them (module the half-window described below).
   * @param args Parameters dictating how `loci` should be partitioned.
   * @tparam R Region type.
   */
  def apply[R <: Region: ClassTag](regions: RDD[R],
                                   loci: LociSet,
                                   args: PartitionedRegionsArgs): PartitionedRegions[R] =
    apply(
      regions,
      LociPartitioning(regions, loci, args),
      args.halfWindowSize,
      args.partitionedReadsPathOpt,
      args.compressReadPartitions,
      args.printPartitioningStats
    )

  /**
   * Internal [[PartitionedRegions]] constructor: takes already-partitioned loci, partitions regions, and optionally
   * prints some stats.
   *
   * If `partitionedRegionsPathOpt` is provided, attempt to load loci- and region- partitionings from that path; if the
   * path doesn't exist, compute them and save to that path.
   */
  private[rdd] def apply[R <: Region: ClassTag](regions: RDD[R],
                                                lociPartitioning: LociPartitioning,
                                                halfWindowSize: Int,
                                                partitionedRegionsPathOpt: Option[String],
                                                compress: Boolean,
                                                printStats: Boolean): PartitionedRegions[R] =
    partitionedRegionsPathOpt match {
      case Some(partitionedRegionsPath) =>

        val sc = regions.sparkContext
        val fs = FileSystem.get(sc.hadoopConfiguration)
        val path = new Path(partitionedRegionsPath)
        if (fs.exists(path))
          load(sc, partitionedRegionsPath, lociPartitioning)
        else
          compute(regions, lociPartitioning, halfWindowSize, compress, printStats)
            .save(partitionedRegionsPath, compressed = compress)

      case None =>
        compute(regions, lociPartitioning, halfWindowSize, compress, printStats)
    }

  /**
   * Construct a [[PartitionedRegions]] for above constructors, ignoring loading/saving considerations.
   */
  private def compute[R <: Region: ClassTag](regions: RDD[R],
                                             lociPartitioning: LociPartitioning,
                                             halfWindowSize: Int,
                                             compress: Boolean,
                                             printStats: Boolean): PartitionedRegions[R] = {

    val sc = regions.sparkContext

    val partitioningBroadcast = sc.broadcast(lociPartitioning)

    val numPartitions = lociPartitioning.numPartitions

    progress(
      s"Partitioning reads into $numPartitions partitions according to loci partitioning:",
      lociPartitioning.truncatedString()
    )

    implicit val accumulableParam = new HistogramParam[Int, Long]

    // Histogram of the number of copies made of each region (i.e. when a region straddles loci-partition
    // boundaries.
    val regionCopiesHistogram: Accumulable[IntHist, Int] = sc.accumulable(IntHist(), "copies-per-region")

    // Histogram of the number of regions assigned to each partition.
    val partitionRegionsHistogram: Accumulable[IntHist, Int] = sc.accumulable(IntHist(), "regions-per-partition")

    val partitionedRegions =
      (for {
        // For each region…
        r <- regions

        // Partitions to send a copy of this region to.
        partitions = partitioningBroadcast.value.getAll(r, halfWindowSize)

        // Add number of copies to histogram accumulator.
        _ = (regionCopiesHistogram += partitions.size)

        // For each partition/copy…
        partition <- partitions
      } yield {
        // Update "regions-per-partition" accumulator.
        partitionRegionsHistogram += partition

        // Key this region with its destination partition, plus secondary and tertiary fields for intra-partition
        // sorting.
        (partition, r.contigName, r.start) -> r
      })
      .repartitionAndSortWithinPartitions(KeyPartitioner(numPartitions))  // Shuffle all region copies
      .values  // Drop keys, leaving just regions.
      .setName("partitioned-regions")

    if (printStats) {
      // Need to force materialization for the accumulators to have data… but that's reasonable because anything
      // downstream is presumably going to reuse this RDD.
      val totalReadCopies = partitionedRegions.count

      // Number of reads before partitioning / selective copying.
      val originalReads = regions.count

      // Sorted array of [number of read copies "K"] -> [number of reads that were copied "K" times].
      val regionCopies: Array[(Int, Long)] = regionCopiesHistogram.value.toArray.sortBy(_._1)

      // Number of distinct reads that were sent to at least one partition.
      val readsPlaced = regionCopies.filter(_._1 > 0).map(_._2).sum

      // Sorted array: [partition index "K"] -> [number of reads assigned to partition "K"].
      val regionsPerPartition: Array[(PartitionIndex, Long)] = partitionRegionsHistogram.value.toArray.sortBy(_._1)

      progress(
        s"Placed $readsPlaced of $originalReads (%.1f%%), %.1fx copies on avg; copies per read histogram:"
          .format(
            100.0 * readsPlaced / originalReads,
            totalReadCopies * 1.0 / readsPlaced
          ),
        Stats.fromHist(regionCopies).toString(),
        "",
        "Reads per partition stats:",
        Stats(regionsPerPartition.map(_._2)).toString()
      )
    }

    new PartitionedRegions(partitionedRegions, lociPartitioning)
  }
}
