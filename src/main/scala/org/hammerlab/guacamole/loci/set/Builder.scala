package org.hammerlab.guacamole.loci.set

import scala.collection.SortedMap

/**
 * Build a LociSet out of Contigs.
 */
private[loci] class Builder {
  private val map = SortedMap.newBuilder[String, Contig]

  def add(contig: Contig): this.type = {
    if (!contig.isEmpty) {
      map += ((contig.name, contig))
    }
    this
  }

  def result: LociSet = {
    LociSet(map.result())
  }
}
