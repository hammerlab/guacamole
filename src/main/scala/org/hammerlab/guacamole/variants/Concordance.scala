package org.hammerlab.guacamole.variants

import java.util.EnumSet

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro.{GenotypeType, Genotype => BDGGenotype}
import org.bdgenomics.qc.rdd.variation.{ConcordanceTable, GenotypeConcordanceRDDFunctions}
import org.hammerlab.guacamole.logging.DebugLogArgs
import org.kohsuke.args4j.{Option => Args4jOption}

/**
 * As a convenience to users experimenting with different callers, some variant callers include functionality to compare
 * the called variants to a gold standard VCF after they are computed, in the same job. We implement that logic here.
 *
 */
object Concordance {

  /**
   * Arguments that callers can include to support concordance calculations.
   */
  trait ConcordanceArgs extends DebugLogArgs {
    @Args4jOption(name = "--truth", metaVar = "truth", usage = "The truth ADAM or VCF genotypes file")
    var truthGenotypesFile: String = ""

    @Args4jOption(name = "--exclude-snv", usage = "Exclude SNV variants in comparison")
    var excludeSNVs: Boolean = false

    @Args4jOption(name = "--exclude-indel", usage = "Exclude indel variants in comparison")
    var excludeIndels: Boolean = false

    @Args4jOption(name = "--chr", usage = "Chromosome to filter to")
    var chromosome: String = ""
  }

  /**
   *
   * Evaluate a set of called genotypes by computing precision, recall and F1-Score
   *
   * Precision = Correct Calls / All Calls Made = Correct Calls / (Correct Calls + Incorrect Calls)
   * Recall =  Correct Calls  / All True Variants = Correct Calls / (Correct Calls + Incorrect Omissions)
   *
   * F1-Score = 2 * (Precision * Recall) / (Precision + Recall)
   *
   * All three metrics range from 0 to 1, where 1 would be perfect.
   *
   * @param calledGenotypes Genotypes to test
   * @param trueGenotypes Known-set of validated genotypes
   * @param excludeSNVs true if want to exclude single nucleotides polymorphism in the evaluation (default: false)
   * @param excludeIndels true if want to exclude insertions in the evaluation in the evaluation (default: true)
   * @param chromosome name of a chromosome, if any, to filter to (default: null)
   * @return  precision, recall and f1score
   */
  def computePrecisionAndRecall(calledGenotypes: RDD[BDGGenotype],
                                trueGenotypes: RDD[BDGGenotype],
                                excludeSNVs: Boolean = false,
                                excludeIndels: Boolean = true,
                                chromosome: String = null): (Double, Double, Double) = {

    def chromosomeFilter(genotype: BDGGenotype): Boolean =
      chromosome == "" ||
        genotype.getVariant.getContig.getContigName == chromosome

    def variantTypeFilter(genotype: BDGGenotype): Boolean = {
      val variant = new RichVariant(genotype.getVariant)
      (
        !excludeSNVs &&
        variant.isSingleNucleotideVariant()
      ) ||
      (
        !excludeIndels &&
        (variant.isInsertion() || variant.isDeletion())
      )
    }

    def relevantVariants(genotype: BDGGenotype): Boolean =
      chromosomeFilter(genotype) &&
        variantTypeFilter(genotype)

    val filteredCalledAlleles = calledGenotypes.filter(relevantVariants)
    val filteredTrueGenotypes = trueGenotypes.filter(relevantVariants)

    val sampleName = filteredCalledAlleles.take(1)(0).getSampleId

    val sampleAccuracy =
      new GenotypeConcordanceRDDFunctions(filteredCalledAlleles)
        .concordanceWith(filteredTrueGenotypes)
        .collectAsMap()(sampleName)

    // We called AND it was called in truth
    val truePositives = sampleAccuracy.total(ConcordanceTable.CALLED, ConcordanceTable.CALLED)

    // We called AND it was NOT called in truth
    val falsePositives = sampleAccuracy.total(ConcordanceTable.CALLED, EnumSet.of(GenotypeType.NO_CALL))

    // We did NOT call AND it was called in truth
    val falseNegatives = sampleAccuracy.total(EnumSet.of(GenotypeType.NO_CALL), ConcordanceTable.CALLED)

    // We did NOT call AND it was NOT called in truth
    // val trueNegatives = sampleAccuracy.total(EnumSet.of(GenotypeType.NO_CALL), EnumSet.of(GenotypeType.NO_CALL))
    // val specificity = trueNegatives / (falsePositives + falseNegatives)
    // Obviously the above won't be recorded ( we don't save NoCall, NoCall info)

    // recall ( aka sensitivity )
    val recall = truePositives.toFloat / (truePositives + falseNegatives)
    val precision = truePositives.toFloat / (truePositives + falsePositives)

    val f1Score = 2.0 * (precision * recall) / (precision + recall)

    (recall, precision, f1Score)
  }

  /**
   *
   * Evaluate a set of called genotypes and print precision, recall and F1-Score
   *
   * @param args parsed arguments
   * @param genotypes ADAM genotypes (i.e. the variants)
   * @param sc spark context
   */

  def printGenotypeConcordance(args: ConcordanceArgs, genotypes: RDD[BDGGenotype], sc: SparkContext) = {
    val trueGenotypes = loadGenotypes(args.truthGenotypesFile, sc)

    val (precision, recall, f1score) =
      computePrecisionAndRecall(
        genotypes,
        trueGenotypes,
        args.excludeSNVs,
        args.excludeIndels,
        args.chromosome
      )

    println("Precision\tRecall\tF1Score")
    println("%f\t%f\t%f".format(precision, recall, f1score))
  }

  /**
   * Load genotypes from ADAM Parquet or VCF file
   *
   * @param path path to VCF or ADAM genotypes
   * @param sc spark context
   * @return RDD of ADAM Genotypes
   */
  def loadGenotypes(path: String, sc: SparkContext): RDD[BDGGenotype] = {
    if (path.endsWith(".vcf")) {
      sc.loadGenotypes(path)
    } else {
      sc.loadParquet(path)
    }
  }
}
