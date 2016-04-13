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

import java.io.{File, InputStreamReader}

import htsjdk.variant.vcf.VCFFileReader
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.hammerlab.guacamole.distributed.LociPartitionUtils
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.set.{LociSet, Builder => LociSetBuilder}
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

  /**
   * Return the loci specified by the user as a LociSetBuilder.
   *
   * @param args parsed arguments
   */
  def lociFromArguments(args: LociArgs, default: String = "all"): LociSetBuilder = {
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
      default
    }

    LociSetBuilder(lociToParse)
  }

  /**
   * Load a LociSet from the specified file, using the contig lengths from the given ReadSet.
   *
   * @param filePath path to file containing loci. If it ends in '.vcf' then it is read as a VCF and the variant sites
   *                 are the loci. If it ends in '.loci' or '.txt' then it should be a file containing loci as
   *                 "chrX:5-10,chr12-10-20", etc. Whitespace is ignored.
   * @param contigLengths contig lengths, by name
   * @return a LociSet
   */
  def lociFromFile(filePath: String, contigLengths: Map[String, Long]): LociSet = {
    if (filePath.endsWith(".vcf")) {
      val builder = new LociSetBuilder
      val reader = new VCFFileReader(new File(filePath), false)
      val iterator = reader.iterator
      while (iterator.hasNext) {
        val value = iterator.next()
        builder.put(value.getContig, value.getStart - 1, value.getEnd.toLong)
      }
      builder.result
    } else if (filePath.endsWith(".loci") || filePath.endsWith(".txt")) {
      val filesystem = FileSystem.get(new Configuration())
      val path = new Path(filePath)
      LociSetBuilder(
        IOUtils.toString(new InputStreamReader(filesystem.open(path)))
      ).result(contigLengths)
    } else {
      throw new IllegalArgumentException(
        s"Couldn't guess format for file: $filePath. Expected file extensions: '.loci' or '.txt' for loci string format; '.vcf' for VCFs."
      )
    }
  }

  /**
   * Load loci from a string or a path to a file.
   *
   * Specify at most one of loci or lociFromFilePath.
   *
   * @param loci loci to load as a string
   * @param lociFromFilePath path to file containing loci to load
   * @param contigLengths contig lengths, by name
   * @return a LociSet
   */
  def loci(loci: String, lociFromFilePath: String, contigLengths: Map[String, Long]): LociSet = {
    if (loci.nonEmpty && lociFromFilePath.nonEmpty) {
      throw new IllegalArgumentException("Specify at most one of the 'loci' and 'loci-from-file' arguments")
    }
    if (loci.nonEmpty) {
      LociSetBuilder(loci).result(Some(contigLengths))
    } else if (lociFromFilePath.nonEmpty) {
      lociFromFile(lociFromFilePath, contigLengths)
    } else {
      // Default is "all"
      LociSet.all(contigLengths)
    }
  }

  /**
   * Parse spark environment variables from commandline. Copied from ADAM.
   *
   * Commandline format is -spark_env foo=1 -spark_env bar=2
   *
   * @param envVariables The variables found on the commandline
   * @return array of (key, value) pairs parsed from the command line.
   */
  def parseEnvVariables(envVariables: Seq[String]): Seq[(String, String)] = {
    envVariables.foldLeft(Seq[(String, String)]()) {
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
  def createSparkContext(appName: String): SparkContext = createSparkContext(Some(appName))
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
      config.set("spark.kryo.registrator", "org.hammerlab.guacamole.kryo.GuacamoleKryoRegistrator")
    }

    if (config.getOption("spark.kryoserializer.buffer").isEmpty) {
      config.set("spark.kryoserializer.buffer", "4mb")
    }

    if (config.getOption("spark.kryo.referenceTracking").isEmpty) {
      config.set("spark.kryo.referenceTracking", "true")
    }

    new SparkContext(config)
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

  /**
   * Perform validation of command line arguments at startup.
   * This allows some late failures (e.g. output file already exists) to be surfaced more quickly.
   */
  def validateArguments(args: GenotypeOutputArgs) = {
    val outputPath = args.variantOutput.stripMargin
    if (outputPath.toLowerCase.endsWith(".vcf")) {
      val filesystem = FileSystem.get(new Configuration())
      val path = new Path(outputPath)
      if (filesystem.exists(path)) {
        throw new FileAlreadyExistsException("Output directory " + path + " already exists")
      }
    }
  }
}

