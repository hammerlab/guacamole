package org.bdgenomics.guacamole

import scala.collection.immutable.NumericRange

object Util {

  def parseLociRanges(value: String): Map[String, Seq[NumericRange[Long]] = {
    val syntax = "([A-z]+):([0-9]+)-([0-9]+)".r
    def apply(value: String): LociRange = value match {
      case syntax(name, start, end) => LociRange(name, start.toLong, end.toLong)
      case _                        => throw new IllegalArgumentException("Couldn't parse: " + value)
    }


  }

}
