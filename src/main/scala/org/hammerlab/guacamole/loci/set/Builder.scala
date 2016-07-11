package org.hammerlab.guacamole.loci.set

import scala.collection.mutable.ArrayBuffer

/**
 * Build a LociSet out of Contigs.
 */
private[loci] class Builder {
  private val map = ArrayBuffer[Contig]()

  def add(contig: Contig): this.type = {
    if (!contig.isEmpty) {
      map += contig
    }
    this
  }

  def result: LociSet = LociSet.fromContigs(map)
}
