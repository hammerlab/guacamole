package org.hammerlab.guacamole.distributed

import org.apache.spark.Partitioner

/**
 * Spark partitioner for keyed RDDs that assigns each unique key its own partition.
 * Used to partition an RDD of (task number: Long, read: MappedRead) pairs, giving each task its own partition.
 *
 * @param numPartitions total number of partitions
 */
case class KeyPartitioner(override val numPartitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = key match {
    case value: Long         => value.toInt
    case value: TaskPosition => value.partition
    case _                   => throw new AssertionError("Unexpected key in PartitionByTask")
  }
  override def equals(other: Any): Boolean = other match {
    case h: KeyPartitioner => h.numPartitions == numPartitions
    case _                 => false
  }
}

