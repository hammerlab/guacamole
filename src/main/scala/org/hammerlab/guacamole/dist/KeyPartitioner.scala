package org.hammerlab.guacamole.dist

import org.apache.spark.Partitioner

/**
  * Spark partitioner for keyed RDDs that assigns each unique key its own partition.
  * Used to partition an RDD of (task number: Long, read: MappedRead) pairs, giving each task its own partition.
  *
  * @param partitions total number of partitions
  */
class KeyPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions
  def getPartition(key: Any): Int = key match {
    case value: Long         => value.toInt
    case value: TaskPosition => value.task
    case _                   => throw new AssertionError("Unexpected key in PartitionByTask")
  }
  override def equals(other: Any): Boolean = other match {
    case h: KeyPartitioner => h.numPartitions == numPartitions
    case _                 => false
  }
}

