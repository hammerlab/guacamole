package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.reference.ContigName

import scala.collection.SortedMap

/**
 * Build a LociSet out of Contigs.
 */
private[loci] class Builder {
  private val map = SortedMap.newBuilder[ContigName, Contig]

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
