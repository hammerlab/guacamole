package org.hammerlab.guacamole.loci.parsing

import scala.collection.mutable.ArrayBuffer

/**
 * Representation of genomic-loci ranges that may be used to instantiate a [[ParsedLoci]] for later conversion into a
 * [[org.hammerlab.guacamole.loci.set.LociSet]].
 *
 * The two implementations are:
 *
 *   - [[All]]: all loci on all contigs.
 *   - [[LociRanges]]: a sequence of [[LociRange]]s denoting (possibly open-ended) genomic-intervals.
 */
sealed trait ParsedLociRanges extends Any

object ParsedLociRanges {
  def apply(lociStrs: String): ParsedLociRanges = ParsedLociRanges(Iterator(lociStrs))

  def apply(lines: Iterator[String]): ParsedLociRanges = {
    val lociRanges = ArrayBuffer[LociRange]()
    for {
      lociStrs <- lines
      lociStr <- lociStrs.replaceAll("\\s", "").split(",")
      lociRange <- ParsedLociRange(lociStr)
    } {
      lociRange match {
        case AllRange => return All
        case lociRange: LociRange =>
          lociRanges += lociRange
      }
    }
    LociRanges(lociRanges)
  }
}

case object All extends ParsedLociRanges

case class LociRanges(ranges: Iterable[LociRange]) extends AnyVal with ParsedLociRanges
