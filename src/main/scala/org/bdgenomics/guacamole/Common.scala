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
import org.bdgenomics.formats.avro.ADAMGenotype
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, Logging, SparkContext }
import org.bdgenomics.adam.rdd.ADAMContext._
import java.util
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.apache.spark.scheduler.StatsReportListener
import java.util.Calendar
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.codehaus.jackson.JsonFactory
import java.io.OutputStream
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.hadoop.conf.Configuration

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

    /** Argument for using / not using sequence dictionaries to get contigs and lengths. */
    trait NoSequenceDictionary extends Base {
      @Opt(name = "-no-sequence-dictionary",
        usage = "If set, get contigs and lengths directly from reads instead of from sequence dictionary.")
      var noSequenceDictionary: Boolean = false
    }

    /** Argument for accepting a single set of reads (for non-somatic variant calling). */
    trait Reads extends Base with NoSequenceDictionary {
      @Opt(name = "-reads", metaVar = "X", required = true, usage = "Aligned reads")
      var reads: String = ""
    }

    /** Arguments for accepting two sets of reads (tumor + normal). */
    trait TumorNormalReads extends Base with NoSequenceDictionary {
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

      @Opt(name = "-out-chunks", metaVar = "X", required = false,
        usage = "When writing out to json format, number of chunks to coalesce the genotypes RDD into.")
      var outChunks: Int = 1

      @Opt(name = "-max-genotypes", metaVar = "X", required = false,
        usage = "Maximum number of genotypes to output. 0 (default) means output all genotypes.")
      var maxGenotypes: Int = 0
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
   * Given arguments for a single set of reads, and a spark context, return a ReadSet.
   *
   * @param args parsed arguments
   * @param sc spark context
   * @param filters input filters to apply
   * @return
   */
  def loadReadsFromArguments(
    args: Arguments.Reads,
    sc: SparkContext,
    filters: Read.InputFilters): ReadSet = {
    ReadSet(sc, args.reads, token = 0, filters = filters, contigLengthsFromDictionary = !args.noSequenceDictionary)
  }

  /**
   * Given arguments for two sets of reads (tumor and normal), return a pair of (tumor, normal) read sets.
   *
   * The 'token' field will be set to 1 in the tumor reads, and 2 in the normal reads.
   *
   * @param args parsed arguments
   * @param sc spark context
   * @param filters input filters to apply
   */
  def loadTumorNormalReadsFromArguments(
    args: Arguments.TumorNormalReads,
    sc: SparkContext,
    filters: Read.InputFilters): (ReadSet, ReadSet) = {

    val tumor = ReadSet(sc, args.tumorReads, filters, 1, !args.noSequenceDictionary)
    val normal = ReadSet(sc, args.normalReads, filters, 2, !args.noSequenceDictionary)
    (tumor, normal)
  }

  /**
   * If the user specifies a -loci argument, parse it out and return the LociSet. Otherwise, construct a LociSet that
   * includes all the loci in the contigs.
   *
   * @param args parsed arguments
   * @param readSet readSet from which to use to get contigs and lengths.
   */
  def loci(args: Arguments.Loci, readSet: ReadSet): LociSet = {
    val result = {
      if (args.loci == "all") {
        // Call at all loci.
        val builder = LociSet.newBuilder
        // Here, pair is (contig name, contig length).
        readSet.contigLengths.foreach(pair => builder.put(pair._1, 0L, pair._2))
        builder.result
      } else {
        // Call at specified loci. Check that loci given are in the sequence dictionary.
        val parsed = LociSet.parse(args.loci)
        parsed.contigs.foreach(contig => {
          if (!readSet.contigLengths.contains(contig))
            throw new IllegalArgumentException("No such contig: '%s'.".format(contig))
        })
        parsed
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
  def writeVariantsFromArguments(args: Arguments.Output, genotypes: RDD[ADAMGenotype]): Unit = {
    val subsetGenotypes = if (args.maxGenotypes > 0) {
      progress("Subsetting to %d genotypes.".format(args.maxGenotypes))
      genotypes.sample(false, args.maxGenotypes, 0)
    } else {
      genotypes
    }
    val outputPath = args.variantOutput.stripMargin
    if (outputPath.isEmpty || outputPath.toLowerCase.endsWith(".json")) {
      val out: OutputStream = if (outputPath.isEmpty) {
        progress("Writing genotypes to stdout.")
        System.out
      } else {
        progress("Writing genotypes serially in json format to: %s.".format(outputPath))
        val filesystem = FileSystem.get(new Configuration())
        val path = new Path(outputPath)
        filesystem.create(path, true)
      }
      val coalescedSubsetGenotypes = if (args.outChunks > 0) subsetGenotypes.coalesce(args.outChunks) else subsetGenotypes
      coalescedSubsetGenotypes.persist()

      // Write each ADAMGenotype with a JsonEncoder.
      val schema = ADAMGenotype.getClassSchema
      val writer = new GenericDatumWriter[Object](schema)
      val encoder = EncoderFactory.get.jsonEncoder(schema, out)
      val generator = new JsonFactory().createJsonGenerator(out)
      generator.useDefaultPrettyPrinter()
      encoder.configure(generator)
      var partitionNum = 0
      val numPartitions = coalescedSubsetGenotypes.partitions.length
      while (partitionNum < numPartitions) {
        progress("Collecting partition %d of %d.".format(partitionNum + 1, numPartitions))
        val chunk = coalescedSubsetGenotypes.mapPartitionsWithIndex((num, genotypes) => {
          if (num == partitionNum) genotypes else Iterator.empty
        }).collect
        chunk.foreach(genotype => {
          writer.write(genotype, encoder)
          encoder.flush()
        })
        partitionNum += 1
      }
      out.write("\n".toByte)
      generator.close()
      coalescedSubsetGenotypes.unpersist()
    } else if (outputPath.toLowerCase.endsWith(".vcf")) {
      progress("Writing genotypes to VCF file: %s.".format(outputPath))
      val sc = subsetGenotypes.sparkContext
      sc.adamVCFSave(outputPath, subsetGenotypes.toADAMVariantContext.coalesce(1))
    } else {
      progress("Writing genotypes to: %s.".format(outputPath))
      subsetGenotypes.adamSave(outputPath,
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
                         sparkUIPort: Option[Int] = None,
                         appName: Option[String] = None): SparkContext = {
    val config: SparkConf = new SparkConf(loadSystemValues).setMaster(args.spark_master)
    appName match {
      case Some(name) => config.setAppName("guacamole: %s".format(name))
      case _          => config.setAppName("guacamole")
    }
    if (args.spark_home != null) config.setSparkHome(args.spark_home)
    if (args.spark_jars.nonEmpty) config.setJars(args.spark_jars)
    if (args.spark_env_vars.nonEmpty) config.setExecutorEnv(parseEnvVariables(args.spark_env_vars))

    // Optionally set the spark driver and UI ports
    sparkDriverPort.foreach(port => config.set("spark.driver.port", port.toString))
    sparkUIPort.foreach(port => config.set("spark.ui.port", port.toString))

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

