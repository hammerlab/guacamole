package org.hammerlab.guacamole.loci.map

import java.lang.{Long => JLong}

import com.google.common.collect.{Range, TreeRangeMap}
import org.hammerlab.guacamole.loci.SimpleRange
import org.hammerlab.guacamole.loci.set.LociSet

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** Helper class for building a LociMap */
private[map] class Builder[T] {
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
      SimpleRange(start, end) <- contig.ranges
    } {
      put(contig.name, start, end, value)
    }
    this
  }

  /** Build the result. */
  def result(): LociMap[T] = {
    LociMap(
      TreeMap(
        (for {
          (contig, array) <- data.toSeq
        } yield {
          val mutableRangeMap = TreeRangeMap.create[JLong, T]()
          array.foreach(item => {
            var (start, end, value) = item
            // If there is an existing entry associated *with the same value* in the map immediately before the range
            // we're adding, we coalesce the two ranges by setting our start to be its start.
            val existingStart = mutableRangeMap.getEntry(start - 1)
            if (existingStart != null && existingStart.getValue == value) {
              assert(existingStart.getKey.lowerEndpoint < start)
              start = existingStart.getKey.lowerEndpoint
            }

            // Likewise for the end of the range.
            val existingEnd = mutableRangeMap.getEntry(end)
            if (existingEnd != null && existingEnd.getValue == value) {
              assert(existingEnd.getKey.upperEndpoint > end)
              end = existingEnd.getKey.upperEndpoint
            }

            mutableRangeMap.put(Range.closedOpen[JLong](start, end), value)
          })
          contig -> Contig(contig, mutableRangeMap)
        }): _*
      )
    )
  }
}

