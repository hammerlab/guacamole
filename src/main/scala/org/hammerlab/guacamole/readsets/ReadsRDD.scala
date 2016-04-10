package org.hammerlab.guacamole.readsets

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.reads.{MappedRead, PairedRead, Read}

case class ReadsRDD(reads: RDD[Read]) {
  lazy val mappedReads =
    reads.flatMap {
      case r: MappedRead                   => Some(r)
      case PairedRead(r: MappedRead, _, _) => Some(r)
      case _                               => None
    }

  lazy val mappedPairedReads: RDD[PairedRead[MappedRead]] =
    reads.flatMap {
      case rp: PairedRead[_] if rp.isMapped => Some(rp.asInstanceOf[PairedRead[MappedRead]])
      case _                                => None
    }
}
