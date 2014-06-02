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
import org.bdgenomics.adam.avro.ADAMGenotype
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, Logging, SparkContext }
import org.bdgenomics.adam.rdd.ADAMContext._
import java.util
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.bdgenomics.adam.models.SequenceDictionary
import org.apache.spark.scheduler.StatsReportListener
import java.util.Calendar

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
      @Opt(name = "-loci", usage = "Loci at which to call variants. Either 'all' or contig:start-end,contig:start-end,...")
      var loci: String = "all"
    }

    /** Argument for accepting a single set of reads (for non-somatic variant calling). */
    trait Reads extends Base {
      @Opt(name = "-reads", metaVar = "X", required = true, usage = "Aligned reads")
      var reads: String = ""
    }

    /** Arguments for accepting two sets of reads (tumor + normal). */
    trait TumorNormalReads extends Base {
      @Opt(name = "-normal-reads", metaVar = "X", required = true, usage = "Aligned reads: normal")
      var normalReads: String = ""

      @Opt(name = "-tumor-reads", metaVar = "X", required = true, usage = "Aligned reads: tumor")
      var tumorReads: String = ""
    }

    /** Argument for writing output genotypes. */
    trait Output extends Base {
      @Opt(name = "-out", metaVar = "VARIANTS_OUT", required = false,
        usage = "Variant output path. If not specified, print to screen.")
      var variantOutput: String = ""
    }

    /** Arguments for accepting a reference genome. */
    trait Reference extends Base {
      @Opt(required = false, name = "-reference", usage = "ADAM or FASTA reference genome data")
      var referenceInput: String = ""

      @Opt(required = false, name = "-fragment_length",
        usage = "Sets maximum fragment length. Default value is 10,000. Values greater than 1e9 should be avoided.")
      var fragmentLength: Long = 10000L
    }
  }

  /**
   * Given arguments for a single set of reads, and a spark context, return an RDD of reads.
   *
   * @param args parsed arguments
   * @param sc spark context
   * @param mapped if true (default), will filter out non-mapped reads
   * @param nonDuplicate if true (default), will filter out duplicate reads.
   * @return
   */
  def loadReadsFromArguments(
    args: Arguments.Reads,
    sc: SparkContext,
    mapped: Boolean = true,
    nonDuplicate: Boolean = true,
    passedQualityChecks: Boolean = true): (RDD[Read], SequenceDictionary) = {

    Read.loadReadRDDAndSequenceDictionaryFromBAM(args.reads, sc, mapped, nonDuplicate, passedQualityChecks)
  }

  /**
   *
   * Load genotypes from ADAM Parquet or VCF file
   *
   * @param path path to VCF or ADAM genotypes
   * @param sc spark context
   * @return RDD of ADAM Genotypes
   */
  def loadGenotypes(path: String, sc: SparkContext): RDD[ADAMGenotype] = {
    if (path.endsWith(".vcf")) {
      sc.adamVCFLoad(path).flatMap(_.genotypes)
    } else {
      sc.adamLoad(path)
    }
  }

  /**
   * Given arguments for two sets of reads (tumor and normal), return a 4-tuple of RDDs of reads and Sequence
   * Dictionaries: (Tumor RDD, Tumor Sequence Dictionary, Normal RDD, Normal Sequence Dictionary).
   *
   * @param args parsed arguments
   * @param sc spark context
   * @param mapped if true, filter out non-mapped reads
   * @param nonDuplicate if true, filter out duplicate reads.
   */
  def loadTumorNormalReadsFromArguments(
    args: Arguments.TumorNormalReads,
    sc: SparkContext,
    mapped: Boolean = true,
    nonDuplicate: Boolean = true): (RDD[Read], SequenceDictionary, RDD[Read], SequenceDictionary) = {
    val (tumorReads, tumorDictionary) = Read.loadReadRDDAndSequenceDictionaryFromBAM(args.tumorReads, sc, mapped, nonDuplicate)
    val (normalReads, normalDictionary) = Read.loadReadRDDAndSequenceDictionaryFromBAM(args.normalReads, sc, mapped, nonDuplicate)
    (tumorReads, tumorDictionary, normalReads, normalDictionary)
  }

  /**
   * If the user specifies a -loci argument, parse it out and return the LociSet. Otherwise, construct a LociSet that
   * includes all the loci spanned by the reads.
   * @param args parsed arguments
   * @param sequenceDictionary Sequence Dictionary giving contig names and lengths.
   */
  def loci(args: Arguments.Loci, sequenceDictionary: SequenceDictionary): LociSet = {
    val result = {
      if (args.loci == "all") {
        // Call at all loci.
        getLociFromAllContigs(sequenceDictionary)
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
   * Collects the full set of loci (i.e. [0, length of contig]) on all contigs in the SequenceDictonary.
   *
   * @param sequenceDictionary ADAM sequence dictionary.
   * @return LociSet of loci included in the sequence dictionary.
   */
  def getLociFromAllContigs(sequenceDictionary: SequenceDictionary): LociSet = {
    val builder = LociSet.newBuilder
    sequenceDictionary.records.foreach(record => {
      builder.put(record.name.toString, 0L, record.length)
    })
    builder.result
  }

  def getLociFromReads(reads: RDD[MappedRead]): LociSet = {
    val contigs = reads.map(read => Map(read.referenceContig -> read.end)).reduce((map1, map2) => {
      val keys = map1.keySet.union(map2.keySet).toSeq
      keys.map(key => key -> math.max(map1.getOrElse(key, 0L), map2.getOrElse(key, 0L))).toMap
    })
    val builder = LociSet.newBuilder
    contigs.foreach(pair => builder.put(pair._1, 0L, pair._2))
    builder.result
  }

  /**
   * Write out an RDD of ADAMGenotype instances to a file.
   * @param args parsed arguments
   * @param genotypes ADAM genotypes (i.e. the variants)
   */
  def writeVariantsFromArguments(args: Arguments.Output, genotypes: RDD[ADAMGenotype]): Unit = {
    val outputPath = args.variantOutput.stripMargin
    if (outputPath.isEmpty) {
      progress("Writing genotypes to stdout.")
      val localGenotypes = genotypes.collect
      localGenotypes.foreach(println _)
    } else if (outputPath.toLowerCase.endsWith(".vcf")) {
      progress("Writing genotypes to VCF file: %s.".format(outputPath))
      val sc = genotypes.sparkContext
      sc.adamVCFSave(outputPath, genotypes.toADAMVariantContext.coalesce(1))
    } else {
      progress("Writing genotypes to: %s.".format(outputPath))
      genotypes.adamSave(outputPath,
        args.blockSize,
        args.pageSize,
        args.compressionCodec,
        args.disableDictionary)
    }
  }

  /**
   * Parse spark environment variables from commandline. Copied from ADAM.
   *
   * Commandline format is -spark_env foo=1 -spark_env bar=2
   * @param envVariables The variables found on the commandline
   * @return array of (key, value) pairs parsed from the command line.
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
  def createSparkContext(args: SparkArgs,
                         loadSystemValues: Boolean = true,
                         sparkDriverPort: Option[Int] = None,
                         appName: Option[String] = None): SparkContext = {
    val config: SparkConf = new SparkConf(loadSystemValues).setMaster(args.spark_master)
    appName match {
      case Some(name) => config.setAppName("guacamole: %s".format(name))
      case _          => config.setAppName("guacamole")
    }
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
    config.setAll(
      Seq(
        ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
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

