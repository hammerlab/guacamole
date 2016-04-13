package org.hammerlab.guacamole.commands

import java.io.{BufferedWriter, FileWriter}

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.distributed.LociPartitionUtils
import org.hammerlab.guacamole.distributed.LociPartitionUtils.partitionLociAccordingToArgs
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMap
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.Read.InputFilters
import org.hammerlab.guacamole.reads.{MappedRead, Read}
import org.hammerlab.guacamole.reference.{ReferenceBroadcast, ReferenceGenome}
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

/**
 * VariantLocus is a locus and the variant allele frequency at that locus
 *
 * @param locus Position of non-reference alleles
 * @param variantAlleleFrequency Frequency of non-reference alleles
 */
case class VariantLocus(contig: String, locus: Long, variantAlleleFrequency: Float)

object VariantLocus {

  /**
   * Construct VariantLocus from a pileup
   *
   * @param pileup Pileup of reads at a given locus
   * @return VariantLocus at reference position, locus
   */
  def apply(pileup: Pileup): Option[VariantLocus] = {
    if (pileup.referenceDepth != pileup.depth) {
      val contig = pileup.elements.head.read.referenceContig
      Some(VariantLocus(contig, pileup.locus, (pileup.depth - pileup.referenceDepth).toFloat / pileup.depth))
    } else {
      None
    }
  }

}

object VAFHistogram {

  protected class Arguments extends LociPartitionUtils.Arguments with Common.Arguments.ReadLoadingConfigArgs {

    @Args4jOption(name = "--out", required = false, forbids = Array("--local-out"),
      usage = "HDFS file path to save the variant allele frequency histogram")
    var output: String = ""

    @Args4jOption(name = "--local-out", required = false, forbids = Array("--out"),
      usage = "Local file path to save the variant allele frequency histogram")
    var localOutputPath: String = ""

    @Args4jOption(name = "--bins", required = false,
      usage = "Number of bins for the variant allele frequency histogram (Default: 20)")
    var bins: Int = 20

    @Args4jOption(name = "--cluster", required = false,
      usage = "Cluster the variant allele frequencies using a Gaussian mixture model")
    var cluster: Boolean = false

    @Args4jOption(name = "--num-clusters", required = false, depends = Array("--cluster"),
      usage = "Number of clusters for the Gaussian mixture model (Default: 3)")
    var numClusters: Int = 3

    @Args4jOption(name = "--min-read-depth", usage = "Minimum read depth to include variant allele frequency")
    var minReadDepth: Int = 0

    @Args4jOption(name = "--min-vaf", usage = "Minimum variant allele frequency to include")
    var minVAF: Int = 0

    @Args4jOption(name = "--print-stats", usage = "Print descriptive statistics on variant allele frequncy distribution")
    var printStats: Boolean = false

    @Args4jOption(name = "--sample-percent", depends = Array("--print-stats"),
      usage = "Percent of variant to use for the calculations (Default: 25)")
    var samplePercent: Int = 25

    @Args4jOption(name = "--reference-fasta", required = true, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = ""

    @Argument(required = true, multiValued = true,
      usage = "BAMs")
    var bams: Array[String] = Array.empty

  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "vaf-histogram"
    override val description = "Compute and cluster the variant allele frequencies"

    override def run(args: Arguments, sc: SparkContext): Unit = {
      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)

      val loci = Common.lociFromArguments(args)
      val filters = Read.InputFilters(
        overlapsLoci = Some(loci),
        nonDuplicate = true,
        passedVendorQualityChecks = true)
      val samplePercent = args.samplePercent

      val readSets: Seq[ReadSet] = args.bams.zipWithIndex.map(
        bamFile =>
          ReadSet(
            sc,
            bamFile._1,
            InputFilters.empty,
            contigLengthsFromDictionary = true,
            config = Common.Arguments.ReadLoadingConfigArgs.fromArguments(args)
          )
      )

      val lociPartitions = partitionLociAccordingToArgs(
        args,
        loci.result(readSets(0).contigLengths),
        readSets(0).mappedReads // Use the first set of reads as a proxy for read depth
      )

      val minReadDepth = args.minReadDepth
      val minVariantAlleleFrequency = args.minVAF
      val variantLoci = readSets.map(readSet =>
        variantLociFromReads(
          readSet.mappedReads,
          reference,
          lociPartitions,
          samplePercent,
          minReadDepth,
          minVariantAlleleFrequency,
          printStats = args.printStats
        )
      )

      val bins = args.bins
      val variantAlleleHistograms =
        variantLoci.map(variantLoci => generateVAFHistogram(variantLoci, bins))

      val sampleAndFileNames = args.bams.zip(readSets.map(_.mappedReads.take(1)(0).sampleName))
      val binSize = 100 / bins

      def histogramToString(kv: (Int, Long)): String = {
        s"${kv._1}, ${math.min(kv._1 + binSize, 100)}, ${kv._2}"
      }

      val histogramOutput =
        sampleAndFileNames
          .zip(variantAlleleHistograms)
          .flatMap(kv => {
            val fileName = kv._1._1
            val sampleName = kv._1._2
            val histogram = kv._2
            histogram.toSeq
              .sortBy(_._1)
              .map(kv => s"$fileName, $sampleName, ${histogramToString(kv)}").toSeq
          })

      if (args.localOutputPath != "") {
        val writer = new BufferedWriter(new FileWriter(args.localOutputPath))
        writer.write("Filename, SampleName, BinStart, BinEnd, Size")
        writer.newLine()
        histogramOutput.foreach(line => {
          writer.write(line)
          writer.newLine()
        })
        writer.flush()
        writer.close()
      } else if (args.output != "") {
        // Parallelize histogram and save on HDFS
        sc.parallelize(histogramOutput).saveAsTextFile(args.output)
      } else {
        // Print histograms to standard out
        variantAlleleHistograms.foreach(histogram =>
          histogram.toSeq.sortBy(_._1).foreach(kv => println(histogramToString(kv)))
        )
      }

      if (args.cluster) {
        val numClusters = args.numClusters
        variantLoci.foreach(buildMixtureModel(_, numClusters))
      }

    }
  }

  /**
   * Generates a count of loci in each variant allele frequency bins
   *
   * @param variantAlleleFrequencies RDD of loci with variant allele frequency > 0
   * @param bins Number of bins to group the VAFs into
   * @return Map of rounded variant allele frequency to number of loci with that value
   */
  def generateVAFHistogram(variantAlleleFrequencies: RDD[VariantLocus], bins: Int): Map[Int, Long] = {
    assume(bins <= 100 && bins >= 1, "Bins should be between 1 and 100")

    def roundToBin(variantAlleleFrequency: Float) = {
      val variantPercent = (variantAlleleFrequency * 100).toInt
      variantPercent - (variantPercent % (100 / bins))
    }
    variantAlleleFrequencies
      .map(vaf => roundToBin(vaf.variantAlleleFrequency) -> 1L)
      .reduceByKey(_ + _)
      .collectAsMap
      .toMap
  }

  /**
   * Find all non-reference loci in the sample
   *
   * @param reads RDD of mapped reads for the sample
   * @param reference genome
   * @param lociPartitions Positions which to examine for non-reference loci
   * @param samplePercent Percent of non-reference loci to use for descriptive statistics
   * @param minReadDepth Minimum read depth before including variant allele frequency
   * @param minVariantAlleleFrequency Minimum variant allele frequency to include
   * @param printStats Print descriptive statistics for the variant allele frequency distribution
   * @return RDD of VariantLocus, which contain the locus and non-zero variant allele frequency
   */
  def variantLociFromReads(reads: RDD[MappedRead],
                           reference: ReferenceGenome,
                           lociPartitions: LociMap[Long],
                           samplePercent: Int = 100,
                           minReadDepth: Int = 0,
                           minVariantAlleleFrequency: Int = 0,
                           printStats: Boolean = false): RDD[VariantLocus] = {
    val sampleName = reads.take(1)(0).sampleName
    val variantLoci =
      pileupFlatMap[VariantLocus](
        reads,
        lociPartitions,
        skipEmpty = true,
        pileup =>
          VariantLocus(pileup)
            .filter(locus => pileup.depth >= minReadDepth)
            .filter(_.variantAlleleFrequency >= (minVariantAlleleFrequency / 100.0))
            .iterator,
        reference
      )
    if (printStats) {
      variantLoci.persist(StorageLevel.MEMORY_ONLY)

      val numVariantLoci = variantLoci.count
      Common.progress("%d non-zero variant loci in sample %s".format(numVariantLoci, sampleName))

      // Sample variant loci to compute descriptive statistics
      val sampledVAFs =
        if (samplePercent < 100)
          variantLoci
            .sample(withReplacement = false, fraction = samplePercent / 100.0)
            .collect()
        else
          variantLoci.collect()

      val stats = new DescriptiveStatistics()
      sampledVAFs.foreach(v => stats.addValue(v.variantAlleleFrequency))

      // Print out descriptive statistics for the variant allele frequency distribution
      Common.progress("Variant loci stats for %s (min: %f, max: %f, median: %f, mean: %f, 25Pct: %f, 75Pct: %f)".format(
        sampleName,
        stats.getMin,
        stats.getMax,
        stats.getPercentile(50),
        stats.getMean,
        stats.getPercentile(25),
        stats.getPercentile(75)
      ))
    }

    variantLoci
  }

  /**
   * Fit a Gaussian mixture model to the distribution of variant allele frequencies
   *
   * @param variantAlleleFrequencies RDD of loci with variant allele frequency > 0
   * @param numClusters Number of Gaussian distributions to fit
   * @param maxIterations Maximum number of iterations to run EM
   * @param convergenceTol Largest change in log-likelihood before convergence
   * @return GaussianMixtureModel
   */
  def buildMixtureModel(variantAlleleFrequencies: RDD[VariantLocus],
                        numClusters: Int,
                        maxIterations: Int = 50,
                        convergenceTol: Double = 1e-2): GaussianMixtureModel = {
    val vafVectors = variantAlleleFrequencies.map(vaf => Vectors.dense(vaf.variantAlleleFrequency))
    val model = new GaussianMixture()
      .setK(numClusters)
      .setConvergenceTol(convergenceTol)
      .setMaxIterations(maxIterations)
      .run(vafVectors)

    for (i <- 0 until model.k) {
      println(s"Cluster $i: mean=${model.gaussians(i).mu(0)}, std. deviation=${model.gaussians(i).sigma}, weight=${model.weights(i)}")
    }

    model
  }

}

