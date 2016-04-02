/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceDictionary
import org.hammerlab.guacamole.reads.{ MappedRead, PairedRead, Read }

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

/**
  * A ReadSet contains an RDD of reads along with some metadata about them.
  *
  * @param reads RDD of reads
  */
case class ReadSet(reads: ReadsRDD,
                   sequenceDictionary: SequenceDictionary,
                   contigLengths: ContigLengths)

object ReadSet {

  /**
    * Load reads from a file.
    *
    * @param sc spark context
    * @param filename bam or sam file to read from
    * @param filters input filters to filter reads while reading.
    * @param contigLengthsFromDictionary if true, the contigLengths property will use the sequence dictionary to get the
    *                                    contig lengths. Otherwise, the reads themselves will be used.
    */
  def apply(sc: SparkContext,
            filename: String,
            filters: Read.InputFilters = Read.InputFilters.empty,
            contigLengthsFromDictionary: Boolean = true,
            config: Read.ReadLoadingConfig = Read.ReadLoadingConfig.default): ReadSet = {
    val ReadSets(readsRDDs, sequenceDictionary, contigLengths) =
      ReadSets(sc, Seq(filename), filters, contigLengthsFromDictionary, config)

    ReadSet(readsRDDs.head, sequenceDictionary, contigLengths)
  }
}
