package org.hammerlab.guacamole.loci.partitioning

import java.io.{InputStream, OutputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.PartitionIndex
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.magic.util.Saveable

import scala.reflect.ClassTag

/**
 * A wrapper around a mapping from genomic-loci ranges to Spark partition numbers.
 *
 * Includes some functionality for saving to / loading from disk.
 */
case class LociPartitioning(map: LociMap[PartitionIndex]) extends Saveable {
  /**
   * Write the wrapped [[map]] to the provided [[OutputStream]].
   */
  override def save(os: OutputStream): Unit = {
    map.prettyPrint(os)
    os.close()
  }

  override def toString: String = map.toString
}

object LociPartitioning {

  def load(is: InputStream): LociPartitioning = LociPartitioning(LociMap.load(is))

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            loci: LociSet,
                                            args: LociPartitionerArgs,
                                            halfWindowSize: Int = 0): LociPartitioning = {
    for (lociPartitioningPath <- args.lociPartitioningPathOpt) {
      val path = new Path(lociPartitioningPath)
      val fs = path.getFileSystem(regions.sparkContext.hadoopConfiguration)
      if (fs.exists(path)) {
        progress(s"Loading loci partitioning from $lociPartitioningPath")
        return load(fs.open(path))
      }
    }

    progress(s"Partitioning loci")
    val lp =
      args
        .getPartitioner(regions, halfWindowSize)
        .partition(loci)

    for (lociPartitioningPath <- args.lociPartitioningPathOpt) {
      progress(s"Saving loci partitioning to $lociPartitioningPath")
      lp.save(
        FileSystem
          .get(regions.sparkContext.hadoopConfiguration)
          .create(new Path(lociPartitioningPath))
      )
    }

    lp
  }

  implicit def lociMapToLociPartitioning(map: LociMap[PartitionIndex]): LociPartitioning = LociPartitioning(map)
  implicit def lociPartitioningToLociMap(partitioning: LociPartitioning): LociMap[PartitionIndex] = partitioning.map
}
