package org.hammerlab.guacamole.reads

/**
 * A mapped read whose mate is also mapped.
 * This allows mate alignment properties to be accessed without Options.
 */
case class PairedMappedRead(read: MappedRead,
                            isFirstInPair: Boolean,
                            inferredInsertSize: Int,
                            mate: MateAlignmentProperties) {
  def readLength(): Long = {
    this.read.sequence.length
  }

  def onSameContig(): Boolean = {
    this.read.referenceContig == this.mate.referenceContig
  }

  // Lowest mapped coordinate in the pair (only makes sense for same contig pairs)
  def minPos(): Long = {
    Math.min(this.read.start, this.mate.start)
  }

  // Greatest mapped coordinate in the pair (only makes sense for same contig pairs)
  def maxPos(): Long = {
    Math.max(this.read.start, this.mate.start) + this.readLength
  }

  // Size of the gap between mated reads (only makes sense for same contig pairs)
  def gapLength(): Long = {
    Math.abs(this.read.start - this.mate.start) + this.readLength
  }

  def ends(): (Long, Long) = {
    (this.minPos, this.maxPos)
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