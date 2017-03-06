package org.hammerlab.guacamole.loci.partitioning

import java.io.{ InputStream, OutputStream }

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.genomics.loci.map.LociMap
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.{ Locus, NumLoci, Region }
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.iterator.LinesIterator
import org.hammerlab.magic.util.Saveable
import org.hammerlab.spark.PartitionIndex
import org.hammerlab.stats.Stats
import org.hammerlab.strings.TruncatedToString

import scala.reflect.ClassTag

/**
 * A wrapper around a mapping from genomic-loci ranges to Spark partition numbers.
 *
 * Includes some functionality for saving to / loading from disk.
 */
case class LociPartitioning(map: LociMap[PartitionIndex])
  extends Saveable
    with TruncatedToString {

  // An RDD[LociSet] with one LociSet per partition.
  def lociSetsRDD(sc: SparkContext): RDD[LociSet] =
    sc
      .parallelize(partitionLociSets, partitionLociSets.length)
      .setName("lociSetsRDD")

  @transient private lazy val partitionLociSets: Array[LociSet] =
    map
      .inverse
      .toArray
      .sortBy(_._1)
      .map(_._2)

  @transient lazy val numPartitions = partitionsMap.size

  @transient lazy val partitionSizeStats = Stats(partitionSizesMap.values.map(_.num))

  @transient lazy val partitionContigStats = Stats(partitionContigsMap.values)

  @transient lazy val partitionSets = partitionsMap.values

  @transient lazy val rangeSizeStats =
    Stats(
      partitionSets
        .flatMap(_.contigs)
        .flatMap(_.ranges)
        .map(_.length.num)
    )

  @transient lazy val partitionRangesStats =
    Stats(
      for {
        partitionLoci ← partitionSets
      } yield
        partitionLoci.contigs.map(_.ranges.length).sum
    )

  @transient private lazy val partitionsMap: Map[PartitionIndex, LociSet] = map.inverse

  @transient private lazy val partitionSizesMap: Map[PartitionIndex, NumLoci] =
    for {
      (partition, loci) ← partitionsMap
    } yield
      partition → loci.count

  @transient private lazy val partitionContigsMap: Map[PartitionIndex, Int] =
    for {
      (partition, loci) ← partitionsMap
    } yield
      partition → loci.contigs.length

  /**
   * Write the wrapped [[map]] to the provided [[OutputStream]].
   */
  override def save(os: OutputStream): Unit = {
    map.prettyPrint(os)
    os.close()
  }

  override def toString: String = map.toString

  override def stringPieces: Iterator[String] = map.stringPieces
}

object LociPartitioning {

  // Build a LociPartitioning with each partition's loci mapped to the partition index.
  def apply(lociSets: Iterable[LociSet]): LociPartitioning = {
    val lociMapBuilder = LociMap.newBuilder[PartitionIndex]
    for {
      (loci, idx) ← lociSets.zipWithIndex
    } {
      lociMapBuilder.put(loci, idx)
    }
    lociMapBuilder.result
  }

  def apply[R <: Region: ClassTag](regions: RDD[R],
                                   loci: LociSet,
                                   args: LociPartitionerArgs): LociPartitioning = {

    val hadoopConfiguration = regions.sparkContext.hadoopConfiguration

    val lociPartitioning: LociPartitioning =
      (for {
        lociPartitioningPath ← args.lociPartitioningPathOpt
        path = new Path(lociPartitioningPath)
        fs = path.getFileSystem(hadoopConfiguration)
        // Load LociPartitioning from disk, if it exists…
        if fs.exists(path)
      } yield {
        progress(s"Loading loci partitioning from $lociPartitioningPath")
        load(fs.open(path))
      })
      .getOrElse({
        // Otherwise, compute it.
        progress(s"Partitioning loci")
        args
          .getPartitioner(regions)
          .partition(loci)
      })

    for (lociPartitioningPath ← args.lociPartitioningPathOpt) {
      progress(s"Saving loci partitioning to $lociPartitioningPath")
      val path = new Path(lociPartitioningPath)
      val fs = path.getFileSystem(hadoopConfiguration)
      lociPartitioning.save(
        fs.create(path)
      )
    }

    progress(
      s"Partitioned loci: ${lociPartitioning.numPartitions} partitions.",
      "Partition-size stats:",
      lociPartitioning.partitionSizeStats.toString(),
      "",
      "Contigs-spanned-per-partition stats:",
      lociPartitioning.partitionContigStats.toString(),
      "",
      "Range-size stats:",
      lociPartitioning.rangeSizeStats.toString(),
      "",
      "Ranges-per-partition stats:",
      lociPartitioning.partitionRangesStats.toString()
    )

    lociPartitioning
  }

  /**
   * Load a LociMap output by [[LociMap.prettyPrint]].
   * @param is [[InputStream]] reading from e.g. a file.
   */
  private def load(is: InputStream): LociPartitioning = fromLines(LinesIterator(is))

  /**
   * Read in a [[LociPartitioning]] of partition indices from some strings, each one representing a genomic range.
   * @param lines string representations of genomic ranges.
   */
  private def fromLines(lines: TraversableOnce[String]): LociPartitioning = {
    val builder = LociMap.newBuilder[PartitionIndex]
    val re = """([^:]+):(\d+)-(\d+)=(\d+)""".r
    for {
      line ← lines
      m ← re.findFirstMatchIn(line)
      contig = m.group(1)
      start = Locus(m.group(2).toLong)
      end = Locus(m.group(3).toLong)
      partition = m.group(4).toInt
    } {
      builder.put(contig, start, end, partition)
    }
    builder.result
  }

  implicit def lociMapToLociPartitioning(map: LociMap[PartitionIndex]): LociPartitioning = LociPartitioning(map)
  implicit def lociPartitioningToLociMap(partitioning: LociPartitioning): LociMap[PartitionIndex] = partitioning.map
}
