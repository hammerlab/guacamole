package org.bdgenomics.guacamole.concordance

import org.bdgenomics.guacamole.{ Common, Command }
import org.apache.spark.Logging
import org.bdgenomics.guacamole.Common.Arguments.VariantConcordance
import org.kohsuke.args4j.Option
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.apache.spark.SparkContext._
import org.bdgenomics.adam.rdd.variation.ConcordanceTable
import java.util.EnumSet
import org.bdgenomics.adam.avro.{ ADAMGenotype, ADAMGenotypeType }
import org.bdgenomics.adam.rich.RichADAMVariant
import org.apache.spark.rdd.RDD

object VariantEvaluator extends Command with Logging {
  val name: String = "varianteval"
  val description: String = "Evaluation script for scoring "

  private class Arguments extends VariantConcordance {

    @Option(name = "-test", required = true, usage = "The test ADAM genotypes file")
    var testGenotypesFile: String = _
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    val calledVariants = Common.loadVariants(args.testGenotypesFile, sc)
    Common.evaluateVariants(args, calledVariants, sc)
  }

  /**
   *
   * Evaluate a set of called variants by computing precision, recall and F1-Score
   *
   * @param calledVariants Variants to test
   * @param truthVariants Known-set of validated variants
   * @param evaluateSNPs true if want to include single nucleotides polymorphism in the evaluation (default: true)
   * @param evaluateINDELs true if want to include insertions in the evaluation in the evaluation (default: false)
   * @param chromosome name of a chromosome, if any, to filter to (default: null)
   * @return  precision, recall and f1score
   */
  def compareVariants(calledVariants: RDD[ADAMGenotype],
                      truthVariants: RDD[ADAMGenotype],
                      evaluateSNPs: Boolean = true,
                      evaluateINDELs: Boolean = false,
                      chromosome: String = null): (Double, Double, Double) = {
    val chromosomeFilter: (ADAMGenotype => Boolean) = chromosome == null || _.variant.contig.contigName.toString == chromosome
    val variantTypeFilter: (ADAMGenotype => Boolean) = genotype => {
      val variant = new RichADAMVariant(genotype.variant)
      (evaluateSNPs && variant.isSingleNucleotideVariant()) || (evaluateINDELs && (variant.isInsertion() || variant.isDeletion()))
    }

    val relevantVariants: (ADAMGenotype => Boolean) = v => chromosomeFilter(v) && variantTypeFilter(v)

    val filteredCalledVariants = calledVariants.filter(relevantVariants)
    val filteredTruthVariants = truthVariants.filter(relevantVariants)

    val sampleName = filteredCalledVariants.take(1)(0).getSampleId.toString
    val sampleAccuracy = filteredCalledVariants.concordanceWith(filteredTruthVariants).collectAsMap()(sampleName)

    // We called AND it was called in truth
    val truePositives = sampleAccuracy.total(ConcordanceTable.CALLED, ConcordanceTable.CALLED)

    // We called AND it was NOT called in truth
    val falsePositives = sampleAccuracy.total(ConcordanceTable.CALLED, EnumSet.of(ADAMGenotypeType.NO_CALL))

    // We did NOT call AND it was called in truth
    val falseNegatives = sampleAccuracy.total(EnumSet.of(ADAMGenotypeType.NO_CALL), ConcordanceTable.CALLED)

    // We did NOT call ANT it was NOT called in truth
    // val trueNegatives = sampleAccuracy.total(EnumSet.of(ADAMGenotypeType.NO_CALL), EnumSet.of(ADAMGenotypeType.NO_CALL))
    // val specificity = trueNegatives / (falsePositives + falseNegatives)
    // Obviously the above won't be recorded ( we don't save NoCall, NoCall info)

    // recall ( aka sensitivity )
    val recall = truePositives.toFloat / (truePositives + falseNegatives)
    val precision = truePositives.toFloat / (truePositives + falsePositives)

    val f1Score = 2.0 * (precision * recall) / (precision + recall)

    (recall, precision, f1Score)
  }

}
