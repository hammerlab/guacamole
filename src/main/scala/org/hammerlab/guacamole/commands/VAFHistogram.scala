package org.hammerlab.guacamole.commands

import java.io.{BufferedWriter, FileWriter}

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapMultipleSamples
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.readsets.args.{Arguments => ReadSetsArguments}
import org.hammerlab.guacamole.readsets.io.{Input, InputFilters, ReadLoadingConfig}
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegions
import org.hammerlab.guacamole.readsets.{NumSamples, PartitionedReads, ReadSets, SampleId}
import org.hammerlab.guacamole.reference.{ContigName, Locus, NumLoci, ReferenceBroadcast, ReferenceGenome}
import org.hammerlab.magic.rdd.SplitByKeyRDD._
import org.kohsuke.args4j.{Option => Args4jOption}

/**
 * VariantLocus is a locus and the variant allele frequency at that locus
 *
 * @param locus Position of non-reference alleles
 * @param variantAlleleFrequency Frequency of non-reference alleles
 */
case class VariantLocus(sampleId: SampleId,
                        contigName: ContigName,
                        locus: Locus,
                        variantAlleleFrequency: Float)

object VariantLocus {

  /**
   * Construct VariantLocus from a pileup
   *
   * @param pileup Pileup of reads at a given locus
   * @return VariantLocus at reference position, locus
   */
  def apply(pileup: Pileup, sampleId: SampleId): Option[VariantLocus] = {
    if (pileup.referenceDepth != pileup.depth) {
      val contigName = pileup.elements.head.read.contigName
      Some(
        VariantLocus(
          sampleId,
          contigName,
          pileup.locus,
          (pileup.depth - pileup.referenceDepth).toFloat / pileup.depth
        )
      )
    } else {
      None
    }
  }
}

object VAFHistogram {

  protected class Arguments extends ReadSetsArguments {

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
  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "vaf-histogram"
    override val description = "Compute and cluster the variant allele frequencies"

    override def run(args: Arguments, sc: SparkContext): Unit = {

      val loci = args.parseLoci(sc.hadoopConfiguration)

      val filters =
        InputFilters(
          overlapsLoci = loci,
          nonDuplicate = true,
          passedVendorQualityChecks = true
        )

      val samplePercent = args.samplePercent

      val readsets =
        ReadSets(
          sc,
          args.inputs,
          filters,
          contigLengthsFromDictionary = true,
          config = ReadLoadingConfig(args)
        )

      val ReadSets(_, _, contigLengths) = readsets

      val mappedReadsRDDs = readsets.mappedReadsRDDs

      val partitionedReads =
        PartitionedRegions(
          readsets.allMappedReads,
          loci.result(contigLengths),
          args,
          halfWindowSize = 0
        )

      val minReadDepth = args.minReadDepth
      val minVariantAlleleFrequency = args.minVAF

      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)

      val (variantLoci, numVariantsPerSample) =
        variantLociFromReads(
          readsets.numSamples,
          partitionedReads,
          reference,
          samplePercent,
          minReadDepth,
          minVariantAlleleFrequency,
          printStats = args.printStats
        )

      val bins = args.bins
      val variantAlleleHistograms = generateVAFHistograms(variantLoci, bins)

      val binSize = 100 / bins

      def histogramEntryString(bin: Int, numLoci: NumLoci): String =
        s"$bin, ${math.min(bin * binSize, 100)}, $numLoci"

      val histogramOutput =
        for {
          Input(sampleId, sampleName, filename) <- args.inputs
          histogram = variantAlleleHistograms(sampleId)
          (bin, numLoci) <- histogram
        } yield
          s"$filename, $sampleName, ${histogramEntryString(bin, numLoci)}"

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
        for {
          (sampleId, histogram) <- variantAlleleHistograms
          (bin, numLoci) <- histogram
        } {
          println(histogramEntryString(bin, numLoci))
        }
      }

      if (args.cluster) {
        val variantsPerSample = variantLoci.keyBy(_.sampleId).splitByKey(numVariantsPerSample)
        for {
          (sampleId, variants) <- variantsPerSample
        } {
          buildMixtureModel(variants, args.numClusters)
        }
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
  def generateVAFHistograms(variantAlleleFrequencies: RDD[VariantLocus],
                            bins: Int): Map[SampleId, Vector[(Int, Long)]] = {
    assume(bins <= 100 && bins >= 1, "Bins should be between 1 and 100")

    def roundToBin(variantAlleleFrequency: Float) = {
      val variantPercent = (variantAlleleFrequency * 100).toInt
      variantPercent - (variantPercent % (100 / bins))
    }

    variantAlleleFrequencies
      .map(vaf => (vaf.sampleId, roundToBin(vaf.variantAlleleFrequency)) -> 1L)
      .reduceByKey(_ + _)
      .map { case ((sampleId, bin), numLoci) => sampleId -> Map(bin -> numLoci) }
      .reduceByKey(_ ++ _)
      .mapValues(_.toVector.sortBy(_._1))
      .collectAsMap
      .toMap
  }

  /**
   * Find all non-reference loci in the sample
   *
   * @param numSamples number of underlying samples represented by `partitionedReads`
   * @param partitionedReads partitioned, mapped reads
   * @param reference genome
   * @param samplePercent Percent of non-reference loci to use for descriptive statistics
   * @param minReadDepth Minimum read depth before including variant allele frequency
   * @param minVariantAlleleFrequency Minimum variant allele frequency to include
   * @param printStats Print descriptive statistics for the variant allele frequency distribution
   * @return RDD of VariantLocus, which contain the locus and non-zero variant allele frequency
   */
  def variantLociFromReads(numSamples: NumSamples,
                           partitionedReads: PartitionedReads,
                           reference: ReferenceGenome,
                           samplePercent: Int = 100,
                           minReadDepth: Int = 0,
                           minVariantAlleleFrequency: Int = 0,
                           printStats: Boolean = false): (RDD[VariantLocus], Map[SampleId, Long]) = {

    val variantLoci =
      pileupFlatMapMultipleSamples[VariantLocus](
        numSamples,
        partitionedReads,
        skipEmpty = true,
        pileups =>
          for {
            (pileup, sampleId) <- pileups.iterator.zipWithIndex
            if pileup.depth >= minReadDepth
            variant <- VariantLocus(pileup, sampleId)
            if variant.variantAlleleFrequency >= (minVariantAlleleFrequency / 100.0)
          } yield
            variant
        ,
        reference
      )

    val numVariantsBySample =
      variantLoci
        .map(_.sampleId)
        .countByValue()
        .toMap

    if (printStats) {
      variantLoci.persist(StorageLevel.MEMORY_ONLY)

      progress(
        "variant loci per sample:",
        (for {
          (sampleId, num) <- numVariantsBySample.toVector.sorted
        } yield
          s"$sampleId:\t$num"
        ).mkString("\n")
      )

      // Sample variant loci to compute descriptive statistics
      val sampledVAFs =
        if (samplePercent < 100)
          variantLoci
            .keyBy(_.sampleId)
            .sampleByKey(
              withReplacement = false,
              fractions = (0 until numSamples).map(_ -> (samplePercent / 100.0)).toMap
            )
            .groupByKey()
            .collect()
        else
          variantLoci
            .groupBy(_.sampleId)
            .collect()

      for {
        (sampleId, variants) <- sampledVAFs
      } {
        val stats = new DescriptiveStatistics()
        variants.foreach(v => stats.addValue(v.variantAlleleFrequency))

        // Print out descriptive statistics for the variant allele frequency distribution
        progress(
          "Variant loci stats for sample %d (min: %f, max: %f, median: %f, mean: %f, 25Pct: %f, 75Pct: %f)".format(
            sampleId,
            stats.getMin,
            stats.getMax,
            stats.getPercentile(50),
            stats.getMean,
            stats.getPercentile(25),
            stats.getPercentile(75)
          )
        )
      }
    }

    (variantLoci, numVariantsBySample)
  }

  /**
   * Fit a Gaussian mixture model to the distribution of variant allele frequencies
   *
   * @param variantLoci RDD of loci with variant allele frequency > 0
   * @param numClusters Number of Gaussian distributions to fit
   * @param maxIterations Maximum number of iterations to run EM
   * @param convergenceTol Largest change in log-likelihood before convergence
   * @return GaussianMixtureModel
   */
  def buildMixtureModel(variantLoci: RDD[VariantLocus],
                        numClusters: Int,
                        maxIterations: Int = 50,
                        convergenceTol: Double = 1e-2): GaussianMixtureModel = {

    val vafVectors =
      variantLoci.map(
        variant =>
          Vectors.dense(variant.variantAlleleFrequency)
      )

    val model =
      new GaussianMixture()
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

