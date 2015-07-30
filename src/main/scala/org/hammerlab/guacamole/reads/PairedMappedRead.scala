package org.hammerlab.guacamole.reads

/**
 * A mapped read whose mate is also mapped.
 * This allows mate alignment properties to be accessed without Options.
 */
case class PairedMappedRead(read: MappedRead,
                            isFirstInPair: Boolean,
                            inferredInsertSize: Int,
                            mate: MateAlignmentProperties) {
  // The length of the primary read's sequence
  def readLength: Long = {
    this.read.sequence.length
  }

  def onSameContig: Boolean = {
    this.read.referenceContig == this.mate.referenceContig
  }

  // Lowest mapped coordinate in the pair (only makes sense for same contig pairs)
  def minPos: Long = {
    Math.min(this.read.start, this.mate.start)
  }

  // Greatest mapped coordinate in the pair (only makes sense for same contig pairs)
  def maxPos: Long = {
    Math.max(this.read.start, this.mate.start) + this.readLength
  }

  // Size of the gap between mated reads (only makes sense for same contig pairs)
  def gapLength: Long = {
    Math.abs(this.read.start - this.mate.start) - this.readLength
  }

  // Should be the same as inferredInsertSize
  def insertSize: Long = {
    this.maxPos - this.minPos
  }

  // Returns the four alignment points, i.e. the start/stop of each read in the pair.
  // The output is guaranteed to be sorted.
  def startsAndStops: (Long, Long, Long, Long) = {
    val r = this.read
    val m = this.mate
    val len = this.readLength
    if (r.start < m.start) {
      (r.start, r.start + len, m.start, m.start + len)
    } else {
      (m.start, m.start + len, r.start, r.start + len)
    }
  }
}

object PairedMappedRead {
  def apply(read: PairedRead[MappedRead]): Option[PairedMappedRead] = {
    for {
      mate <- read.mateAlignmentProperties
      inferredInsertSize <- mate.inferredInsertSize
    } yield PairedMappedRead(read.read, read.isFirstInPair, inferredInsertSize, mate)
  }
}
