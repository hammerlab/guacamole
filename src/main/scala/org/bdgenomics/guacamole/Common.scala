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

import java.io.{ InputStreamReader, OutputStream }
import java.util
import java.util.Calendar

import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Logging, SparkConf, SparkContext }
import org.bdgenomics.adam.cli.{ Args4jBase, ParquetArgs }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.guacamole.Concordance.ConcordanceArgs
import org.bdgenomics.guacamole.reads.Read
import org.codehaus.jackson.JsonFactory
import org.kohsuke.args4j.{ Option => Opt }

/**
 * Collection of functions that are useful to multiple variant calling implementations, and specifications of command-
 * line arguments that they use.
 *
 */
object Common extends Logging {
  object Arguments {
    /** Common argument(s) we always want.*/
    trait Base extends Args4jBase with ParquetArgs {
      @Opt(name = "--debug", usage = "If set, prints a higher level of debug output.")
      var debug = false
    }

    /** Argument for accepting a set of loci. */
    trait Loci extends Base {
      @Opt(name = "--loci", usage = "Loci at which to call variants. Either 'all' or contig:start-end,contig:start-end,...", forbids = Array("-loci-from-file"))
      var loci: String = ""

      @Opt(name = "--loci-from-file", usage = "Path to file giving loci at which to call variants.", forbids = Array("-loci"))
      var lociFromFile: String = ""
    }

    /** Argument for using / not using sequence dictionaries to get contigs and lengths. */
    trait NoSequenceDictionary extends Base {
      @Opt(name = "--no-sequence-dictionary",
        usage = "If set, get contigs and lengths directly from reads instead of from sequence dictionary.")
      var noSequenceDictionary: Boolean = false
    }

    /** Argument for accepting a single set of reads (for non-somatic variant calling). */
    trait Reads extends Base with NoSequenceDictionary {
      @Opt(name = "--reads", metaVar = "X", required = true, usage = "Aligned reads")
      var reads: String = ""
    }

    /** Arguments for accepting two sets of reads (tumor + normal). */
    trait TumorNormalReads extends Base with NoSequenceDictionary {
      @Opt(name = "--normal-reads", metaVar = "X", required = true, usage = "Aligned reads: normal")
      var normalReads: String = ""

      @Opt(name = "--tumor-reads", metaVar = "X", required = true, usage = "Aligned reads: tumor")
      var tumorReads: String = ""
    }

    /** Argument for writing output genotypes. */
    trait Output extends Base {
      @Opt(name = "--out", metaVar = "VARIANTS_OUT", required = false,
        usage = "Variant output path. If not specified, print to screen.")
      var variantOutput: String = ""

      @Opt(name = "--out-chunks", metaVar = "X", required = false,
        usage = "When writing out to json format, number of chunks to coalesce the genotypes RDD into.")
      var outChunks: Int = 1

      @Opt(name = "--max-genotypes", metaVar = "X", required = false,
        usage = "Maximum number of genotypes to output. 0 (default) means output all genotypes.")
      var maxGenotypes: Int = 0
    }

    /** Arguments for accepting a reference genome. */
    trait Reference extends Base {
      @Opt(required = false, name = "--reference", usage = "ADAM or FASTA reference genome data")
      var referenceInput: String = ""

      @Opt(required = false, name = "--fragment-length",
        usage = "Sets maximum fragment length. Default value is 10,000. Values greater than 1e9 should be avoided.")
      var fragmentLength: Long = 10000L
    }

    trait GermlineCallerArgs extends Output with Reads with ConcordanceArgs with DistributedUtil.Arguments

    trait SomaticCallerArgs extends Output with TumorNormalReads with DistributedUtil.Arguments

  }

  /**
   *
   * Load genotypes from ADAM Parquet or VCF file
   *
   * @param path path to VCF or ADAM genotypes
   * @param sc spark context
   * @return RDD of ADAM Genotypes
   */
  def loadGenotypes(path: String, sc: SparkContext): RDD[Genotype] = {
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
    ReadSet(sc, args.reads, filters, token = 0, contigLengthsFromDictionary = !args.noSequenceDictionary)
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
    if (args.loci.nonEmpty && args.lociFromFile.nonEmpty) {
      throw new IllegalArgumentException("Specify at most one of the 'loci' and 'loci-from-file' arguments")
    }
    val lociToParse = if (args.loci.nonEmpty) {
      args.loci
    } else if (args.lociFromFile.nonEmpty) {
      // Load loci from file.
      val filesystem = FileSystem.get(new Configuration())
      val path = new Path(args.lociFromFile)
      IOUtils.toString(new InputStreamReader(filesystem.open(path)))
    } else {
      // Default is "all"
      "all"
    }

    val result = {
      if (lociToParse == "all") {
        // Call at all loci.
        val builder = LociSet.newBuilder
        readSet.contigLengths.foreach(contigNameAndLength => builder.put(contigNameAndLength._1, 0L, contigNameAndLength._2))
        builder.result
      } else {
        // Call at specified loci.
        val parsed = LociSet.parse(lociToParse)

        // Check that loci given are in the sequence dictionary.
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
   * Write out an RDD of Genotype instances to a file.
   * @param args parsed arguments
   * @param genotypes ADAM genotypes (i.e. the variants)
   */
  def writeVariantsFromArguments(args: Arguments.Output, genotypes: RDD[Genotype]): Unit = {
    val subsetGenotypes = if (args.maxGenotypes > 0) {
      progress("Subsetting to %d genotypes.".format(args.maxGenotypes))
      genotypes.sample(withReplacement = false, args.maxGenotypes, 0)
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

      // Write each Genotype with a JsonEncoder.
      val schema = Genotype.getClassSchema
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
      out.write("\n".getBytes())
      generator.close()
      coalescedSubsetGenotypes.unpersist()
    } else if (outputPath.toLowerCase.endsWith(".vcf")) {
      progress("Writing genotypes to VCF file: %s.".format(outputPath))
      val sc = subsetGenotypes.sparkContext
      sc.adamVCFSave(outputPath, subsetGenotypes.toVariantContext.coalesce(1, shuffle = true))
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
   *
   * Return a spark context.
   *
   * NOTE: Most properties are set through config file
   *
   * @param appName
   * @return
   */
  def createSparkContext(appName: Option[String] = None): SparkContext = {
    val config: SparkConf = new SparkConf()
    appName match {
      case Some(name) => config.setAppName("guacamole: %s".format(name))
      case _          => config.setAppName("guacamole")
    }

    if (config.getOption("spark.master").isEmpty) {
      config.setMaster("local[%d]".format(Runtime.getRuntime.availableProcessors()))
    }

    if (config.getOption("spark.serializer").isEmpty) {
      config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    }

    if (config.getOption("spark.kryo.registrator").isEmpty) {
      config.set("spark.kryo.registrator", "org.bdgenomics.guacamole.GuacamoleKryoRegistrator")
    }

    if (config.getOption("spark.kryoserializer.buffer.mb").isEmpty) {
      config.set("spark.kryoserializer.buffer.mb", "4")
    }

    if (config.getOption("spark.kryo.referenceTracking").isEmpty) {
      config.set("spark.kryo.referenceTracking", "true")
    }

    new SparkContext(config)
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

