package org.hammerlab.guacamole.loci.map

import java.lang.{Long => JLong}

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.Interval

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** Helper class for building a LociMap */
private[loci] class Builder[T] {
  private val data = new mutable.HashMap[String, ArrayBuffer[(JLong, JLong, T)]]()

  /** Set the value at the given locus range in the LociMap under construction. */
  def put(contig: String, start: Long, end: Long, value: T): Builder[T] = {
    assume(end >= start)
    if (end > start) {
      data
        .getOrElseUpdate(contig, ArrayBuffer())
        .append((start, end, value))
    }
    this
  }

  /** Set the value for all loci in the given LociSet to the specified value in the LociMap under construction. */
  def put(loci: LociSet, value: T): Builder[T] = {
    for {
      contig <- loci.contigs
      Interval(start, end) <- contig.ranges
    } {
      put(contig.name, start, end, value)
    }
    this
  }

  /** Build the result. */
  def result(): LociMap[T] = LociMap.fromContigs(data.map(Contig(_)))
}

