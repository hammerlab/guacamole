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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Logging, SparkContext}
import org.hammerlab.guacamole.distributed.LociPartitionUtils
import org.hammerlab.guacamole.logging.DebugLogArgs
import org.hammerlab.guacamole.reads.{InputFilters, ReadLoadingConfigArgs}
import org.hammerlab.guacamole.variants.Concordance.ConcordanceArgs
import org.hammerlab.guacamole.variants.GenotypeOutputArgs
import org.kohsuke.args4j.{Option => Args4jOption}

/**
 * Basic functions that most commands need, and specifications of command-line arguments that they use.
 *
 */
object Common extends Logging {
  /** Argument for using / not using sequence dictionaries to get contigs and lengths. */
  trait NoSequenceDictionaryArgs extends DebugLogArgs {
    @Args4jOption(
      name = "--no-sequence-dictionary",
      usage = "If set, get contigs and lengths directly from reads instead of from sequence dictionary."
    )
    var noSequenceDictionary: Boolean = false
  }

  /** Argument for accepting a single set of reads (for non-somatic variant calling). */
  trait ReadsArgs extends DebugLogArgs with NoSequenceDictionaryArgs with ReadLoadingConfigArgs {
    @Args4jOption(name = "--reads", metaVar = "X", required = true, usage = "Aligned reads")
    var reads: String = ""
  }

  /** Arguments for accepting two sets of reads (tumor + normal). */
  trait TumorNormalReadsArgs extends DebugLogArgs with NoSequenceDictionaryArgs with ReadLoadingConfigArgs {
    @Args4jOption(name = "--normal-reads", metaVar = "X", required = true, usage = "Aligned reads: normal")
    var normalReads: String = ""

    @Args4jOption(name = "--tumor-reads", metaVar = "X", required = true, usage = "Aligned reads: tumor")
    var tumorReads: String = ""
  }

  trait GermlineCallerArgs extends GenotypeOutputArgs with ReadsArgs with ConcordanceArgs with LociPartitionUtils.Arguments

  trait SomaticCallerArgs extends GenotypeOutputArgs with TumorNormalReadsArgs with LociPartitionUtils.Arguments

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
                             filters: InputFilters): ReadSet = {
    ReadSet(
      sc,
      args.reads,
      filters,
      contigLengthsFromDictionary = !args.noSequenceDictionary,
      config = ReadLoadingConfigArgs(args)
    )
  }

  /**
   * Given arguments for two sets of reads (tumor and normal), return a pair of (tumor, normal) read sets.
   *
   * @param args parsed arguments
   * @param sc spark context
   * @param filters input filters to apply
   */
  def loadTumorNormalReadsFromArguments(args: TumorNormalReadsArgs,
                                        sc: SparkContext,
                                        filters: InputFilters): (ReadSet, ReadSet) = {

    val tumor = ReadSet(
      sc,
      args.tumorReads,
      filters,
      !args.noSequenceDictionary,
      ReadLoadingConfigArgs(args)
    )

    val normal = ReadSet(
      sc,
      args.normalReads,
      filters,
      !args.noSequenceDictionary,
      ReadLoadingConfigArgs(args)
    )

    (tumor, normal)
  }
}

