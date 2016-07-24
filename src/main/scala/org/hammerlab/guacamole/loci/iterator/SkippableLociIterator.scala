package org.hammerlab.guacamole.loci.iterator

import org.hammerlab.guacamole.reference.Locus
import org.hammerlab.magic.iterator.SimpleBufferedIterator

abstract class SkippableLociIterator[T] extends SimpleBufferedIterator[T] {

  var locus: Locus = 0

  override def postNext(): Unit = {
    locus += 1
  }

  def skipTo(newLocus: Locus): this.type = {
    if (newLocus > locus) {
      locus = newLocus
      clear()
    }
    this
  }

}
