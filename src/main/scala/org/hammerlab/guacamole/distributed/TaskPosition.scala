package org.hammerlab.guacamole.distributed

import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.PartitionIndex
import org.hammerlab.guacamole.reference.{ContigName, Locus}

/**
 * TaskPosition represents the task a read is assigned to and the start position on the reference genome of the read
 * Each read is assigned to a task and the reads are sorted by (referenceContig, locus) when they are processed
 *
 * @param partition Partition index
 * @param contigName Reference or chromosome name for reads
 * @param locus The position in the reference contig at which the read starts
 */
case class TaskPosition(partition: PartitionIndex,
                        contigName: ContigName,
                        locus: Locus)
  extends Ordered[TaskPosition] {

  // Sorting is performed by first sorting on task, secondly on contig and lastly on the start locus
  override def compare(other: TaskPosition): Int = {
    if (partition != other.partition) {
      partition - other.partition
    } else {
      val contigComparison = contigName.compare(other.contigName)
      if (contigComparison != 0) {
        contigComparison
      } else {
        (locus - other.locus).toInt
      }
    }
  }
}

