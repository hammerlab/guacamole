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

package org.hammerlab.guacamole.readsets

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceDictionary
import org.hammerlab.guacamole.ContigLengths
import org.hammerlab.guacamole.reads.{MappedRead, Read, ReadLoadingConfig}

/**
 * A ReadSet contains an RDD of reads along with some metadata about them. It is basically a ReadSets restricted to one
 * input RDD.
 *
 * @param reads RDD of reads
 */
case class ReadSet(reads: ReadsRDD,
                   sequenceDictionary: SequenceDictionary,
                   contigLengths: ContigLengths)

object ReadSet {

  /**
   * Load reads from a file. Delegates to ReadSets, pulls out the one RDD of Reads, and exposes it directly.
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
            config: ReadLoadingConfig = ReadLoadingConfig.default): ReadSet = {

    val ReadSets(readsRDDs, sequenceDictionary, contigLengths) =
      ReadSets(sc, Seq(filename), filters, contigLengthsFromDictionary, config)

    ReadSet(readsRDDs.head, sequenceDictionary, contigLengths)
  }

  /**
   * Given arguments for a single set of reads, and a spark context, return a ReadSet.
   *
   * @param args parsed arguments
   * @param sc spark context
   * @param filters input filters to apply
   * @return
   */
  def loadReadsFromArguments(args: ReadsArgs,
                             sc: SparkContext,
                             filters: Read.InputFilters): ReadSet = {
    ReadSet(
      sc,
      args.reads,
      filters,
      contigLengthsFromDictionary = !args.noSequenceDictionary,
      config = ReadLoadingConfigArgs.fromArguments(args)
    )
  }

  /**
   * Load just the mapped reads from an input ReadSet.
   */
  def loadMappedReadsFromArguments(args: ReadsArgs,
                                   sc: SparkContext,
                                   filters: Read.InputFilters): (RDD[MappedRead], ContigLengths) = {
    val ReadSet(reads, _, contigLengths) =
      ReadSet(
        sc,
        args.reads,
        filters,
        contigLengthsFromDictionary = !args.noSequenceDictionary,
        config = ReadLoadingConfigArgs.fromArguments(args)
      )

    (reads.mappedReads, contigLengths)
  }
}
