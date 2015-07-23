package org.hammerlab.guacamole.reads

/**
 * A mapped read whose mate is also mapped to the same reference contig.
 * This allows mate alignment properties to be accessed without Options.
 */
case class PairedMappedRead(read: MappedRead,
                            isFirstInPair: Boolean,
                            inferredInsertSize: Int,
                            mate: MateAlignmentProperties) {
}

object PairedMappedRead {
  def apply(read: PairedRead[MappedRead]): Option[PairedMappedRead] = {
    for {
      mate <- read.mateAlignmentProperties
      inferredInsertSize <- mate.inferredInsertSize
    } yield PairedMappedRead(read.read, read.isFirstInPair, inferredInsertSize, mate)
  }
}