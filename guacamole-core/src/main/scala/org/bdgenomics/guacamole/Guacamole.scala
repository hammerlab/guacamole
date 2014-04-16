/*
 * Copyright (c) 2013-2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.guacamole

import org.apache.commons.configuration.HierarchicalConfiguration
import org.apache.commons.configuration.plist.PropertyListConfiguration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkContext, Logging }
import org.kohsuke.args4j.{ Option => option, Argument }
import org.bdgenomics.adam.avro.{ ADAMVariant, ADAMRecord, ADAMNucleotideContigFragment }
import org.bdgenomics.adam.cli.{
  ADAMSparkCommand,
  ADAMCommandCompanion,
  ParquetArgs,
  SparkArgs,
  Args4j,
  Args4jBase
}
import org.bdgenomics.adam.models.{ ADAMVariantContext, ReferenceRegion }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.predicates.LocusPredicate
import org.bdgenomics.guacamole.callers.AbsurdlyAggressiveVariantCaller

object guacamole extends ADAMCommandCompanion {

  val commandName = "guacamole"
  val commandDescription = "Call variants using guacamole."

  def apply(args: Array[String]) = {
    new guacamole(Args4j[guacamoleArgs](args))
  }
}

class guacamoleArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(metaVar = "READS", required = true, usage = "ADAM read-oriented data", index = 0)
  var readInput: String = ""

  @Argument(metaVar = "VARIANTS_OUT", required = true, usage = "ADAM variant output", index = 1)
  var variantOutput: String = ""

  @option(required = false, name = "reference", usage = "ADAM or FASTA reference genome data")
  var referenceInput: String = ""

  @option(name = "-parallelism", usage = "Number of variant calling tasks to use. Set to 0 (currently the default) to call variants on the Spark master node, with no parallelism.")
  var parallelism: Int = 0

  @option(name = "-loci", usage = "Loci at which to call variants. Format: contig:start-end,contig:start-end,...")
  var loci: String = ""

  @option(name = "-sort", usage = "Sort reads: use if reads are not already stored sorted.")
  var sort = true

  @option(required = false, name = "-fragment_length", usage = "Sets maximum fragment length. Default value is 10,000. Values greater than 1e9 should be avoided.")
  var fragmentLength: Long = 10000L

  @option(name = "-debug", usage = "If set, prints a higher level of debug output.")
  var debug = false
}

class guacamole(protected val args: guacamoleArgs) extends ADAMSparkCommand[guacamoleArgs] with Logging {

  // companion object to this class - needed for ADAMCommand framework
  val companion = guacamole

  /**
   * Main method. SparkContext and Hadoop Job are provided by the ADAMSparkCommand shell.
   *
   * @param sc SparkContext for RDDs.
   * @param job Hadoop Job container for file I/O.
   */
  def run(sc: SparkContext, job: Job) {
    log.info("Starting.")

    val reference = {
      if (args.referenceInput.isEmpty) None
      else Some(sc.adamSequenceLoad(args.referenceInput, args.fragmentLength))
    }

    val reads: RDD[ADAMRecord] = sc.adamLoad(args.readInput, Some(classOf[LocusPredicate]))

    log.info("Loaded %d reference fragments and %d reads".format(
      reference.map(_.count).getOrElse(0),
      reads.count))

    val samples = reads.map(_.getRecordGroupSample).distinct.collect.map(_.toString).toSet
    val caller = new AbsurdlyAggressiveVariantCaller(samples)
    val loci: LociRanges = {
      if (args.loci.isEmpty) {
        // Call at all loci.
        val contigsAndLengths = reads.map(read => (read.getReferenceName.toString, read.getReferenceLength)).distinct.collect
        assume(contigsAndLengths.map(_._1).distinct.length == contigsAndLengths.length,
          "Some contigs have different lengths in reads: " + contigsAndLengths.toString)
        LociRanges(contigsAndLengths.toSeq.map({ case (contig, length) => (contig, 0.toLong, length.toLong) }))
      } else {
        // Call at specified loci.
        LociRanges.parse(args.loci)
      }
    }
    val genotypes = InvokeVariantCaller.usingSpark(reads, caller, loci, args.parallelism)

    // save variants to output file
    log.info("Writing calls to disk.")
    genotypes.adamSave(args.variantOutput,
      args.blockSize,
      args.pageSize,
      args.compressionCodec,
      args.disableDictionary)
  }
}
