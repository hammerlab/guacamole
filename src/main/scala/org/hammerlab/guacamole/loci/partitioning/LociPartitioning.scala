package org.hammerlab.guacamole.loci.partitioning

import java.io.{InputStream, OutputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.PartitionIndex
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.reference.{NumLoci, ReferenceRegion}
import org.hammerlab.magic.iterator.LinesIterator
import org.hammerlab.magic.stats.Stats
import org.hammerlab.magic.util.Saveable

import scala.reflect.ClassTag

/**
 * A wrapper around a mapping from genomic-loci ranges to Spark partition numbers.
 *
 * Includes some functionality for saving to / loading from disk.
 */
case class LociPartitioning(map: LociMap[PartitionIndex]) extends Saveable {

  @transient lazy val numPartitions = partitionsMap.size

  @transient lazy val partitionSizeStats = Stats(partitionSizesMap.values)

  @transient lazy val partitionContigStats = Stats(partitionContigsMap.values)

  @transient private lazy val partitionsMap: Map[PartitionIndex, LociSet] = map.inverse

  @transient private lazy val partitionSizesMap: Map[PartitionIndex, NumLoci] =
    for {
      (partition, loci) <- partitionsMap
    } yield
      partition -> loci.count

  @transient private lazy val partitionContigsMap: Map[PartitionIndex, Int] =
    for {
      (partition, loci) <- partitionsMap
    } yield
      partition -> loci.contigs.length

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

  /**
   * Load a LociMap output by [[LociMap.prettyPrint]].
   * @param is [[InputStream]] reading from e.g. a file.
   */
  def load(is: InputStream): LociPartitioning = {
    fromLines(LinesIterator(is))
  }

  /**
   * Read in a [[LociPartitioning]] of partition indices from some strings, each one representing a genomic range.
   * @param lines string representations of genomic ranges.
   */
  def fromLines(lines: TraversableOnce[String]): LociPartitioning = {
    val builder = LociMap.newBuilder[PartitionIndex]
    val re = """([^:]+):(\d+)-(\d+)=(\d+)""".r
    for {
      line <- lines
      m <- re.findFirstMatchIn(line)
      contig = m.group(1)
      start = m.group(2).toLong
      end = m.group(3).toLong
      partition = m.group(4).toInt
    } {
      builder.put(contig, start, end, partition)
    }
    builder.result()
  }

  // Build a LociPartitioning with each partition's loci mapped to the partition index.
  def apply(lociSets: Iterable[LociSet]): LociPartitioning = {
    val lociMapBuilder = LociMap.newBuilder[PartitionIndex]
    for {
      (loci, idx) <- lociSets.zipWithIndex
    } {
      lociMapBuilder.put(loci, idx)
    }
    lociMapBuilder.result()
  }

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
