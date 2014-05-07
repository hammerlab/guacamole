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

package org.bdgenomics.guacamole

import org.bdgenomics.adam.cli.{ SparkArgs, ParquetArgs, Args4jBase }
import org.kohsuke.args4j.{ Option => Opt }
import org.bdgenomics.adam.avro.{ ADAMGenotype, ADAMRecord }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.predicates.UniqueMappedReadPredicate
import org.apache.spark.{ SparkConf, Logging, SparkContext }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.scheduler.StatsReportListener
import java.util
import java.util.Calendar
import org.bdgenomics.adam.rich.RichADAMRecord

/**
 * Collection of functions that are useful to multiple variant calling implementations, and specifications of command-
 * line arguments that they use.
 *
 */
object Common extends Logging {
  object Arguments {
    /** Common argument(s) we always want.*/
    trait Base extends Args4jBase with ParquetArgs with SparkArgs {
      @Opt(name = "-debug", usage = "If set, prints a higher level of debug output.")
      var debug = false
    }

    /** Argument for accepting a set of loci. */
    trait Loci extends Base {
      @Opt(name = "-loci", usage = "Loci at which to call variants. One of 'all', 'mapped', or contig:start-end,contig:start-end,...")
      var loci: String = "mapped"
    }

    /** Argument for accepting a single set of reads (for non-somatic variant calling). */
    trait Reads extends Base {
      @Opt(name = "-reads", metaVar = "X", required = true, usage = "Aligned reads")
      var reads: String = ""
    }

    /** Arguments for accepting two sets of reads (tumor + normal). */
    trait TumorNormalReads extends Base {
      @Opt(name = "-reads-normal", metaVar = "X", required = true, usage = "Aligned reads: normal")
      var readsNormal: String = ""

      @Opt(name = "-reads-tumor", metaVar = "X", required = true, usage = "Aligned reads: tumor")
      var readTumor: String = ""
    }

    /** Argument for writing output genotypes. */
    trait Output extends Base {
      @Opt(name = "-out", metaVar = "VARIANTS_OUT", required = true, usage = "Variant output")
      var variantOutput: String = ""
    }

    /** Arguments for accepting a reference genome. */
    trait Reference extends Base {
      @Opt(required = false, name = "-reference", usage = "ADAM or FASTA reference genome data")
      var referenceInput: String = ""

      @Opt(required = false, name = "-fragment_length", usage = "Sets maximum fragment length. Default value is 10,000. Values greater than 1e9 should be avoided.")
      var fragmentLength: Long = 10000L
    }
  }

  /**
   * Given arguments for a single set of reads, and a spark context, return an RDD of reads.
   *
   * @param args parsed arguments
   * @param sc spark context
   * @param mapped if true, will filter out non-mapped reads
   * @param nonDuplicate if true, will filter out duplicate reads.
   * @return
   */
  def loadReads(args: Arguments.Reads, sc: SparkContext, mapped: Boolean = true, nonDuplicate: Boolean = true): RDD[ADAMRecord] = {
    val reads: RDD[ADAMRecord] = if (mapped && nonDuplicate) {
      sc.adamLoad(args.reads, Some(classOf[UniqueMappedReadPredicate]))
    } else {
      var raw: RDD[ADAMRecord] = sc.adamLoad(args.reads)
      if (mapped) raw = raw.filter(read => read.readMapped && read.contig.contigName != null && read.contig.contigLength > 0)
      if (nonDuplicate) raw = raw.filter(read => !read.duplicateRead)
      raw
    }
    reads.persist()
    val description = (if (mapped) "mapped " else "") + (if (nonDuplicate) "non-duplicate" else "")
    progress("Loaded %,d %s reads into %,d partitions.".format(reads.count, description, reads.partitions.length))
    reads
  }

  /**
   * If the user specifies a -loci argument, parse it out and return the LociSet. Otherwise, construct a LociSet that
   * includes all the loci spanned by the reads.
   * @param args parsed arguments
   * @param reads RDD of ADAM reads to use if the user didn't specify loci in the arguments.
   */
  def loci(args: Arguments.Loci, reads: RDD[ADAMRecord]): LociSet = {
    val result = {
      if (args.loci == "all") {
        // Call at all loci.
        progress("Considering all loci on all contigs.")
        val contigsAndLengths = reads.map(read => (read.contig.contigName.toString, read.contig.contigLength)).distinct.collect.toSeq
        assume(contigsAndLengths.map(_._1).distinct.length == contigsAndLengths.length,
          "Some contigs have different lengths in reads: " + contigsAndLengths.toString)
        val builder = LociSet.newBuilder
        contigsAndLengths.foreach({ case (contig, length) => builder.put(contig, 0L, length.toLong) })
        builder.result
      } else if (args.loci == "mapped") {
        progress("Considering all loci with mapped reads.")
        reads.mapPartitions(iterator => {
          val builder = LociSet.newBuilder
          iterator.foreach(read => builder.put(read.contig.contigName.toString, read.start, read.end.get))
          Seq(builder.result).iterator
        }).reduce(LociSet.union(_, _))
      } else {
        // Call at specified loci.
        LociSet.parse(args.loci)
      }
    }
    progress("Including %,d loci across %,d contig(s): %s".format(
      result.count,
      result.contigs.length,
      result.truncatedString()))
    result
  }

  /**
   * Write out an RDD of ADAMGenotype instances to a file.
   * @param args parsed arguments
   * @param genotypes ADAM genotypes (i.e. the variants)
   */
  def writeVariants(args: Arguments.Output, genotypes: RDD[ADAMGenotype]): Unit = {
    progress("Writing %,d genotypes to: %s.".format(genotypes.count, args.variantOutput))
    genotypes.adamSave(args.variantOutput,
      args.blockSize,
      args.pageSize,
      args.compressionCodec,
      args.disableDictionary)
    progress("Done writing.")
  }

  /**
   * Parse spark environment variables from commandline. Copied from ADAM.
   *
   * Commandline format is -spark_env foo=1 -spark_env bar=2
   * @param envVariables The variables found on the commandline
   * @return
   */
  def parseEnvVariables(envVariables: util.ArrayList[String]): Array[(String, String)] = {
    envVariables.foldLeft(Array[(String, String)]()) {
      (a, kv) =>
        val kvSplit = kv.split("=")
        if (kvSplit.size != 2) {
          throw new IllegalArgumentException("Env variables should be key=value syntax, e.g. -spark_env foo=bar")
        }
        a :+ (kvSplit(0), kvSplit(1))
    }
  }

  /**
   * Return a spark context. Copied from ADAM so we can set the Kryo serializer.
   * @param args parsed arguments
   * @param loadSystemValues
   * @param sparkDriverPort
   */
  def createSparkContext(args: SparkArgs, loadSystemValues: Boolean = true, sparkDriverPort: Option[Int] = None): SparkContext = {
    val config: SparkConf = new SparkConf(loadSystemValues).setAppName("guacamole").setMaster(args.spark_master)
    if (args.spark_home != null) config.setSparkHome(args.spark_home)
    if (args.spark_jars != Nil) config.setJars(args.spark_jars)
    if (args.spark_env_vars != Nil) config.setExecutorEnv(parseEnvVariables(args.spark_env_vars))

    // Optionally set the spark driver port
    sparkDriverPort match {
      case Some(port) => config.set("spark.driver.port", port.toString)
      case None       =>
    }

    // Setup the Kryo settings
    // The spark.kryo.registrator setting below is our only modification from ADAM's version of this function.
    config.setAll(Array[(String, String)](("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
      ("spark.kryo.registrator", "org.bdgenomics.guacamole.GuacamoleKryoRegistrator"),
      ("spark.kryoserializer.buffer.mb", args.spark_kryo_buffer_size.toString),
      ("spark.kryo.referenceTracking", "false")))

    val sc = new SparkContext(config)
    if (args.spark_add_stats_listener) {
      sc.addSparkListener(new StatsReportListener)
    }
    sc
  }

  /** Time in milliseconds of last progress message. */
  var lastProgressTime: Long = 0

  /**
   * Print or log a progress message. For now, we just print to standard out, since ADAM's logging setup makes it
   * difficult to see log messages at the INFO level without flooding ourselves with parquet messages.
   * @param message String to print or log.
   */
  def progress(message: String): Unit = {
    val current = System.currentTimeMillis
    val time = if (lastProgressTime == 0)
      Calendar.getInstance.getTime.toString
    else
      "%.2f sec. later".format((current - lastProgressTime) / 1000.0)
    println("--> [%15s]: %s".format(time, message))
    System.out.flush()
    lastProgressTime = current
  }

  /**
   * Does the given read overlap any of the given loci, with halfWindowSize padding?
   */
  def overlapsLociSet(read: RichADAMRecord, loci: LociSet, halfWindowSize: Long = 0): Boolean = {
    read.readMapped && loci.onContig(read.contig.contigName.toString).intersects(
      math.max(0, read.start - halfWindowSize),
      read.end.get + halfWindowSize)
  }

  /**
   * Does the given read overlap the given locus, with halfWindowSize padding?
   */
  def overlapsLocus(read: RichADAMRecord, locus: Long, halfWindowSize: Long = 0): Boolean = {
    read.readMapped && read.start - halfWindowSize <= locus && read.end.get + halfWindowSize > locus
  }

  /**
   * Like Scala's List.mkString method, but supports truncation.
   *
   * Return the concatenation of an iterator over strings, separated by separator, truncated to at most maxLength
   * characters. If truncation occurs, the string is terminated with ellipses.
   */
  def assembleTruncatedString(
    pieces: Iterator[String],
    maxLength: Int,
    separator: String = ",",
    ellipses: String = " [...]"): String = {
    val builder = StringBuilder.newBuilder
    var remaining: Int = maxLength
    while (pieces.hasNext && remaining > ellipses.length) {
      val string = pieces.next()
      builder.append(string)
      if (pieces.hasNext) builder.append(separator)
      remaining -= string.length + separator.length
    }
    if (pieces.hasNext) builder.append(ellipses)
    builder.result
  }
}
