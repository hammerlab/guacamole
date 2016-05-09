package org.hammerlab.guacamole.readsets

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.reads.{MappedRead, PairedRead, Read}

/**
 * A thin wrapper around an RDD[Read], with helpers to filter to mapped and paired-mapped reads.
 */
case class ReadsRDD(reads: RDD[Read], sourceFile: String) {
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

object ReadsRDD {
  def apply(pair: (RDD[Read], String)): ReadsRDD = ReadsRDD(pair._1, pair._2)
}
