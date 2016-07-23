package org.hammerlab.guacamole.readsets.rdd

import java.io.File

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.reads.{MappedRead, PairedRead, Read}
import org.hammerlab.guacamole.readsets.io.Input

/**
 * A thin wrapper around an RDD[Read], with helpers to filter to mapped and paired-mapped reads.
 */
case class ReadsRDD(reads: RDD[Read], input: Input) {

  val basename = new File(input.path).getName
  val shortName = basename.substring(0, math.min(100, basename.length))

  lazy val mappedReads =
    reads.flatMap({
      case r: MappedRead                   => Some(r)
      case PairedRead(r: MappedRead, _, _) => Some(r)
      case _                               => None
    }).setName(s"Mapped reads: $shortName")

  lazy val mappedPairedReads: RDD[PairedRead[MappedRead]] =
    reads.flatMap({
      case rp: PairedRead[_] if rp.isMapped => Some(rp.asInstanceOf[PairedRead[MappedRead]])
      case _                                => None
    }).setName(s"Mapped reads: $shortName")
}
