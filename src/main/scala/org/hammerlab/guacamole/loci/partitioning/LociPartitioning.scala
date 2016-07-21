package org.hammerlab.guacamole.loci.partitioning

import java.io.{InputStream, OutputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.PartitionIndex
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{Locus, ReferenceRegion}
import org.hammerlab.magic.util.{Saveable, Stats}

import scala.reflect.ClassTag

case class LociPartitioning(map: LociMap[PartitionIndex]) extends Saveable {
  @transient lazy val partitionsMap: Map[PartitionIndex, LociSet] = map.inverse
  @transient lazy val numPartitions = partitionsMap.size
  @transient lazy val partitionSizesMap: Map[PartitionIndex, Locus] =
    for {
      (partition, loci) <- partitionsMap
    } yield
      partition -> loci.count

  @transient lazy val partitionSizeStats = Stats(partitionSizesMap.values)

  @transient lazy val partitionContigsMap: Map[PartitionIndex, Int] =
    for {
      (partition, loci) <- partitionsMap
    } yield
      partition -> loci.contigs.length

  @transient lazy val partitionContigStats = Stats(partitionContigsMap.values)

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
        return load(fs.open(path))
      }
    }

    val lp =
      args
        .getPartitioner(regions, halfWindowSize)
        .partition(loci)

    for (lociPartitioningPath <- args.lociPartitioningPathOpt) {
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
