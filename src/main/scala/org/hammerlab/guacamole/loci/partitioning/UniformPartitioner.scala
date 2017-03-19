package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.SparkContext
import org.hammerlab.genomics.loci.map.LociMap
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.guacamole.loci.partitioning.MicroRegionPartitioner.NumMicroPartitions
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.spark.NumPartitions
import org.kohsuke.args4j.{ Option ⇒ Args4jOption }
import org.scalautils.ConversionCheckedTripleEquals._
import spire.implicits._
import spire.math.Integral

import scala.math.{ max, round }

trait UniformPartitionerArgs {
  @Args4jOption(
    name = "--parallelism",
    usage = "Number of variant calling partitions. Set to 0 (default) to use the number of Spark partitions."
  )
  protected var parallelism: NumPartitions = 0

  def numPartitions(sc: SparkContext) =
    if (parallelism == 0)
      sc.defaultParallelism
    else
      parallelism
}

/**
 * Base class for partitioning genomes "uniformly", i.e. so that each partition gets assigned approximately the same
 * number of loci.
 *
 * Subclasses can partition a genome directly into an ([[Int]]) number of Spark partitions (see [[UniformPartitioner]]),
 * or more finely into a ([[Long]]) set of "micro-partitions" for subsequent aggregation into suitable Spark partitions
 * (see [[UniformMicroPartitioner]] and its use by [[MicroRegionPartitioner]]).
 *
 * @param numPartitions number of partitions.
 * @tparam N [[Long]] or [[Int]]; provides some type-safety to use-cases where we compute Spark partitions directly vs.
 *          not.
 */
private[partitioning] sealed abstract class UniformPartitionerBase[N: Integral](numPartitions: N) {

  /**
   * Assign loci to partitions. Contiguous intervals of loci will tend to get assigned to the same partition.
   *
   * Loci are assigned uniformly, i.e. each partition gets about the same number of loci.
   *
   * @return LociMap of locus → partition indices.
   */
  def partitionsMap(loci: LociSet): LociMap[N] = {

    assume(numPartitions >= 1, "`numPartitions` (--parallelism) should be >= 1")

    val lociPerPartition = max(1, loci.count.toDouble / numPartitions.toDouble)

    progress(
      "Splitting loci evenly among %,d numPartitions = ~%,.0f loci per partition"
      .format(numPartitions.toLong(), lociPerPartition)
    )

    var lociAssigned = NumLoci(0)

    var partition = Integral[N].zero

    def remainingForThisPartition = NumLoci(round((partition + 1).toDouble * lociPerPartition) - lociAssigned)

    val builder = LociMap.newBuilder[N]

    for {
      contig ← loci.contigs
      range ← contig.ranges
    } {
      var start = range.start
      val end = range.end
      while (start < end) {
        val length = remainingForThisPartition.min(end - start)
        builder.put(contig.name, start, start + length, partition)
        start += length
        lociAssigned += length
        if (remainingForThisPartition === NumLoci(0)) partition += 1
      }
    }

    val result = builder.result
    assert(lociAssigned === loci.count)
    assert(result.count === loci.count)
    result
  }
}

/**
 * [[UniformPartitionerBase]] implementation that computes "micro-partitions" that can be aggregated into higher-level,
 * varyingly-sized partitions; see [[MicroRegionPartitioner]].
 */
case class UniformMicroPartitioner(numPartitions: NumMicroPartitions)
  extends UniformPartitionerBase[NumMicroPartitions](numPartitions)

/**
 * [[UniformPartitionerBase]] implementation of [[LociPartitioner]] that computes Spark partitions directly.
 */
case class UniformPartitioner(numPartitions: NumPartitions)
  extends UniformPartitionerBase[NumPartitions](numPartitions)
    with LociPartitioner {
  override def partition(loci: LociSet): LociPartitioning = partitionsMap(loci)
}
