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

package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.{ LociMap, Common, LociSet, SlidingReadWindow, DistributedUtil }
import org.bdgenomics.guacamole.Common.progress
import org.bdgenomics.adam.rich.RichADAMRecord._
import org.bdgenomics.adam.avro.{ ADAMRecord, ADAMGenotype }
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.kohsuke.args4j.{ Option => Opt }
import scala.Option
import org.bdgenomics.guacamole.Common.Arguments.{ Loci, Base }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.RichADAMRecord
import org.apache.spark.Logging
import scala.collection.immutable.NumericRange

trait SlidingWindowVariantCaller {
  /**
   * The size of the sliding window (number of bases to either side of a locus) requested by this variant caller
   * implementation.
   *
   * Implementations must override this.
   *
   */
  val halfWindowSize: Long

  /**
   * Given a the samples to call variants for, a [[SlidingReadWindow]], and loci on one contig to call variants at, returns an iterator of
   * genotypes giving the result of variant calling. The [[SlidingReadWindow]] will have the window size requested
   * by this variant caller implementation.
   *
   * Implementations must override this.
   *
   */
  def callVariants(samples: Seq[String], reads: SlidingReadWindow, loci: LociSet.SingleContig): Iterator[ADAMGenotype]
}
object SlidingWindowVariantCaller extends Logging {
  trait Arguments extends Base with Loci {
    @Opt(name = "-no-sort", usage = "Don't sort reads. Use if reads are already stored sorted.")
    var noSort: Boolean = false

    @Opt(name = "-parallelism", usage = "Num variant calling tasks. Set to 0 (default) to call variants on the Spark master")
    var parallelism: Int = 0

  }

  /**
   * Call variants using the given SlidingWindowVariantCaller.
   *
   * @param args parsed commandline arguments
   * @param caller a variant caller instance
   * @param reads reads to use to call variants
   * @return the variants
   */
  def invoke(args: Arguments, caller: SlidingWindowVariantCaller, reads: RDD[ADAMRecord]): RDD[ADAMGenotype] = {
    val loci = Common.loci(args, reads)

    val includedReads = reads.filter(read => DistributedUtil.overlaps(RichADAMRecord(read), loci, caller.halfWindowSize))
    progress("Filtered to %d reads that overlap loci of interest.".format(includedReads.count))

    val samples = reads.map(read => Option(read.recordGroupSample).map(_.toString).getOrElse("default")).distinct.collect
    progress("Reads contain %d sample(s): %s".format(samples.length, samples.mkString(",")))

    // Sort reads by start.
    val sorted = if (!args.noSort) includedReads.adamSortReadsByReferencePosition else includedReads

    DistributedUtil.windowTaskFlatMap(sorted, loci, caller.halfWindowSize, args.parallelism, (task, taskLoci, taskReads) => {
      val readsSplitByContig = DistributedUtil.splitReadsByContig(taskReads.iterator, taskLoci.contigs)
      val slidingWindows = readsSplitByContig.mapValues(SlidingReadWindow(caller.halfWindowSize, _))

      // Reads are coming in sorted by contig, so we process one contig at a time, in order.
      val genotypes = slidingWindows.toSeq.sortBy(_._1).flatMap({
        case (contig, window) => caller.callVariants(samples, window, taskLoci.onContig(contig))
      })
      genotypes
    })
  }
}
