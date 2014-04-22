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

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkContext, Logging }
import java.util.logging.Level
import org.kohsuke.args4j.{ CmdLineException, Option, CmdLineParser, Argument }
import org.bdgenomics.adam.avro.{ ADAMVariant, ADAMRecord, ADAMNucleotideContigFragment }
import org.bdgenomics.adam.cli._
import org.bdgenomics.adam.models.{ ADAMVariantContext, ReferenceRegion }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.predicates.LocusPredicate
import org.bdgenomics.guacamole.callers.{ VariantCallerFactory, VariantCaller, ThresholdVariantCaller }
import org.bdgenomics.adam.util.{ HadoopUtil, ParquetLogger }
import org.bdgenomics.guacamole.Util.progress
import scala.Some
import scala.util.control.Exception._
import scala.Some
import org.bdgenomics.adam.rdd.ADAMContext

/**
 * These arguments are common to all variant callers.
 */
class GuacamoleCommonArguments extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(metaVar = "READS", required = true, usage = "Aligned reads", index = 0)
  var readInput: String = ""

  @Argument(metaVar = "VARIANTS_OUT", required = true, usage = "Variant output", index = 1)
  var variantOutput: String = ""

  @Option(required = false, name = "-reference", usage = "ADAM or FASTA reference genome data")
  var referenceInput: String = ""

  @Option(name = "-parallelism", usage = "Number of variant calling tasks to use. Set to 0 (currently the default) to call variants on the Spark master node, with no parallelism.")
  var parallelism: Int = 0

  @Option(name = "-loci", usage = "Loci at which to call variants. Format: contig:start-end,contig:start-end,...")
  var loci: String = ""

  @Option(name = "-sort", usage = "Sort reads: use if reads are not already stored sorted.")
  var sort = true

  @Option(required = false, name = "-fragment_length", usage = "Sets maximum fragment length. Default value is 10,000. Values greater than 1e9 should be avoided.")
  var fragmentLength: Long = 10000L

  @Option(name = "-debug", usage = "If set, prints a higher level of debug output.")
  var debug = false
}

/**
 * Guacamole main class.
 */
object Guacamole extends Logging {

  private val variantCallers: Seq[VariantCallerFactory] = List(
    ThresholdVariantCaller)

  private def printUsage() = {
    println("Usage: java ... <variant-caller> <reads.align.adam> <output.gt.adam> [other args]\n")
    println("Available variant callers:")
    variantCallers.foreach(caller => {
      println("%10s: %s".format(caller.name, caller.description))
    })
    println("\nTry java ... <variant-caller> -h for help on a particular variant caller.")
  }

  private def createSparkContext(args: SparkArgs): SparkContext = {
    ADAMContext.createSparkContext(
      "guacamole",
      args.spark_master,
      args.spark_home,
      args.spark_jars,
      args.spark_env_vars,
      args.spark_add_stats_listener,
      args.spark_kryo_buffer_size)
  }

  /**
   * Entry point into Guacamole.
   * @param args command line arguments
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      printUsage()
      System.exit(1)
    } else {
      val callerName = args(0)
      val rest = args.drop(1)
      val (parsedArgs: GuacamoleCommonArguments, instance: VariantCaller) = variantCallers.find(_.name == callerName) match {
        case Some(callerFactory) => callerFactory.fromCommandLineArguments(rest)
        case None => {
          println("Unknown variant caller: %s".format(callerName))
          printUsage()
          System.exit(1)
        }
      }
      ParquetLogger.hadoopLoggerLevel(Level.SEVERE) // Quiet parquet logging.
      val sparkContext = createSparkContext(parsedArgs)
      val job = HadoopUtil.newJob()
      run(sparkContext, job, parsedArgs, instance)
    }
  }

  private def run(sc: SparkContext, job: Job, args: GuacamoleCommonArguments, caller: VariantCaller) = {
    progress("Guacamole starting.")

    val reference = {
      if (args.referenceInput.isEmpty) None
      else Some(sc.adamSequenceLoad(args.referenceInput, args.fragmentLength))
    }

    val rawReads: RDD[ADAMRecord] = sc.adamLoad(args.readInput, Some(classOf[LocusPredicate]))
    progress("Loaded %d reads and %d reference fragments.".format(rawReads.count, reference.map(_.count).getOrElse(0)))

    val reads = rawReads.filter(read =>
      read.readMapped &&
        !read.duplicateRead &&
        read.referenceName != null &&
        read.referenceLength > 0)
    progress("Filtered to %d valid, non-duplicate, mapped reads.".format(reads.count))

    val loci: LociSet = {
      if (args.loci.isEmpty) {
        // Call at all loci.
        val contigsAndLengths = reads.map(read => (read.referenceName.toString, read.referenceLength)).distinct.collect
        assume(contigsAndLengths.map(_._1).distinct.length == contigsAndLengths.length,
          "Some contigs have different lengths in reads: " + contigsAndLengths.toString)
        LociSet(contigsAndLengths.toSeq.map({ case (contig, length) => (contig, 0.toLong, length.toLong) }))
      } else {
        // Call at specified loci.
        LociSet.parse(args.loci)
      }
    }
    progress("Considering %d loci across %d contig(s).".format(loci.count, loci.contigs.length))
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
