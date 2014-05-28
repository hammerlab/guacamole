package org.bdgenomics.guacamole.concordance

import org.bdgenomics.guacamole.{ Common, Command }
import org.apache.spark.{ SparkContext, Logging }
import org.kohsuke.args4j.Option
import org.bdgenomics.adam.cli.{ GenotypeConcordance, Args4j }
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.apache.spark.SparkContext._
import org.bdgenomics.adam.rdd.variation.ConcordanceTable
import java.util.EnumSet
import org.bdgenomics.adam.avro.{ ADAMGenotype, ADAMGenotypeType }
import org.bdgenomics.adam.rich.RichADAMVariant
import org.apache.spark.rdd.RDD

object GenotypesEvaluator extends Command with Logging {
  val name: String = "varianteval"
  val description: String = "Evaluation script for scoring"

  trait GenotypeConcordance extends Common.Arguments.Base {

    @Option(name = "-truth", metaVar = "truth", usage = "The truth ADAM or VCF genotypes file")
    var truthGenotypesFile: String = _

    @Option(name = "-include-snv", usage = "Include SNV variants in comparison")
    var includeSNVs: Boolean = false
    @Option(name = "-include-indel", usage = "Include indel variants in comparison")
    var includeIndels: Boolean = false
    @Option(name = "-chr", usage = "Chromosome to filter to")
    var chromosome: String = ""
  }

  private class Arguments extends GenotypeConcordance {

    @Option(name = "-test", required = true, usage = "The test ADAM genotypes file")
    var testGenotypesFile: String = _
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    val calledGenotypes = Common.loadGenotypes(args.testGenotypesFile, sc)
    evaluateGenotypes(args, calledGenotypes, sc)
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
   * @param includeSNVs true if want to include single nucleotides polymorphism in the evaluation (default: true)
   * @param includeIndels true if want to include insertions in the evaluation in the evaluation (default: false)
   * @param chromosome name of a chromosome, if any, to filter to (default: null)
   * @return  precision, recall and f1score
   */
  def computePrecisionAndRecall(calledGenotypes: RDD[ADAMGenotype],
                                trueGenotypes: RDD[ADAMGenotype],
                                includeSNVs: Boolean = true,
                                includeIndels: Boolean = false,
                                chromosome: String = null): (Double, Double, Double) = {
    val chromosomeFilter: (ADAMGenotype => Boolean) = chromosome == "" || _.variant.contig.contigName.toString == chromosome
    val variantTypeFilter: (ADAMGenotype => Boolean) = genotype => {
      val variant = new RichADAMVariant(genotype.variant)
      (includeSNVs && variant.isSingleNucleotideVariant()) || (includeIndels && (variant.isInsertion() || variant.isDeletion()))
    }

    val relevantVariants: (ADAMGenotype => Boolean) = v => chromosomeFilter(v) && variantTypeFilter(v)

    val filteredCalledGenotypes = calledGenotypes.filter(relevantVariants)
    val filteredTrueGenotypes = trueGenotypes.filter(relevantVariants)

    val sampleName = filteredCalledGenotypes.take(1)(0).getSampleId.toString
    val sampleAccuracy = filteredCalledGenotypes.concordanceWith(filteredTrueGenotypes).collectAsMap()(sampleName)

    // We called AND it was called in truth
    val truePositives = sampleAccuracy.total(ConcordanceTable.CALLED, ConcordanceTable.CALLED)

    // We called AND it was NOT called in truth
    val falsePositives = sampleAccuracy.total(ConcordanceTable.CALLED, EnumSet.of(ADAMGenotypeType.NO_CALL))

    // We did NOT call AND it was called in truth
    val falseNegatives = sampleAccuracy.total(EnumSet.of(ADAMGenotypeType.NO_CALL), ConcordanceTable.CALLED)

    // We did NOT call AND it was NOT called in truth
    // val trueNegatives = sampleAccuracy.total(EnumSet.of(ADAMGenotypeType.NO_CALL), EnumSet.of(ADAMGenotypeType.NO_CALL))
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

  def evaluateGenotypes(args: GenotypeConcordance, genotypes: RDD[ADAMGenotype], sc: SparkContext) = {
    val trueGenotypes = Common.loadGenotypes(args.truthGenotypesFile, sc)

    val (precision, recall, f1score) = computePrecisionAndRecall(genotypes, trueGenotypes, args.includeSNVs, args.includeIndels, args.chromosome)
    println("Precision\tRecall\tF1Score")
    println("%f\t%f\t%f".format(precision, recall, f1score))
  }

}
