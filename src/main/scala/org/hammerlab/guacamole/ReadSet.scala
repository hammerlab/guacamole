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
import org.hammerlab.guacamole.reads.{ MappedRead, Read, PairedRead }

/**
 * A ReadSet contains an RDD of reads along with some metadata about them.
 *
 * @param reads RDD of reads
 * @param sequenceDictionary optional, sequence dictionary giving contigs and lengths
 * @param source string describing where these reads came from. Usually a filename.
 * @param filters filters used when reading these reads.
 * @param token token field for all of the reads in this set.
 * @param contigLengthsFromDictionary if true, the contigLengths property will use the sequence dictionary to get the
 *                                    contig lengths. Otherwise, the reads themselves will be used.
 */
case class ReadSet(
    reads: RDD[Read],
    sequenceDictionary: Option[SequenceDictionary],
    source: String,
    filters: Read.InputFilters,
    token: Int,
    contigLengthsFromDictionary: Boolean) {

  /** Only mapped reads. */
  lazy val mappedReads = reads.flatMap(read =>
    read match {
      case r: MappedRead                   => Some(r)
      case PairedRead(r: MappedRead, _, _) => Some(r)
      case _                               => None
    }
  )

  lazy val mappedPairedReads: RDD[PairedRead[MappedRead]] = reads.flatMap(read =>
    read match {
      case rp: PairedRead[_] if rp.isMapped => Some(rp.asInstanceOf[PairedRead[MappedRead]])
      case _                                => None
    }
  )

  /**
   * A map from contig name -> length of contig.
   *
   * Can come either from the sequence dictionary, or by looking at the reads themselves, according to the
   * contigLengthsFromDictionary property.
   *
   */
  lazy val contigLengths: Map[String, Long] = {
    if (contigLengthsFromDictionary) {
      assume(sequenceDictionary.isDefined)
      val builder = Map.newBuilder[String, Long]
      sequenceDictionary.get.records.foreach(record => builder += ((record.name.toString, record.length)))
      builder.result
    } else {
      mappedReads.map(read => Map(read.referenceContig -> read.end)).reduce((map1, map2) => {
        val keys = map1.keySet.union(map2.keySet).toSeq
        keys.map(key => key -> math.max(map1.getOrElse(key, 0L), map2.getOrElse(key, 0L))).toMap
      }).toMap
    }
  }
}
object ReadSet {
  /**
   * Load reads from a file.
   *
   * @param sc spark context
   * @param filename bam or sam file to read from
   * @param requireMDTagsOnMappedReads*
   * @param filters input filters to filter reads while reading.
   * @param token token field for the reads
   * @param contigLengthsFromDictionary see [[ReadSet]] doc for description
   *
   * @return
   */
  def apply(
    sc: SparkContext,
    filename: String,
    requireMDTagsOnMappedReads: Boolean,
    filters: Read.InputFilters = Read.InputFilters.empty,
    token: Int = 0,
    contigLengthsFromDictionary: Boolean = true): ReadSet = {

    val (reads, sequenceDictionary) =
      Read.loadReadRDDAndSequenceDictionary(
        filename,
        sc,
        token = token,
        filters = filters,
        requireMDTagsOnMappedReads
      )

    new ReadSet(reads, Some(sequenceDictionary), filename, filters, token, contigLengthsFromDictionary)
  }
}
