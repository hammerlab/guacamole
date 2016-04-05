package org.hammerlab.guacamole.dist

/**
  * TaskPosition represents the task a read is assigned to and the start position on the reference genome of the read
  * Each read is assigned to a task and the reads are sorted by (referenceContig, locus) when they are processed
  *
  * @param task Task ID
  * @param referenceContig Reference or chromosome name for reads
  * @param locus The position in the reference contig at which the read starts
  */
case class TaskPosition(task: Int, referenceContig: String, locus: Long) extends Ordered[TaskPosition] {

  // Sorting is performed by first sorting on task, secondly on contig and lastly on the start locus
  override def compare(other: TaskPosition): Int = {
    if (task != other.task) {
      task - other.task
    } else {
      val contigComparison = referenceContig.compare(other.referenceContig)
      if (contigComparison != 0) {
        contigComparison
      } else {
        (locus - other.locus).toInt
      }
    }
  }
}

