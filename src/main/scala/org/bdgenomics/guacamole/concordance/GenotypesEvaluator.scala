package org.bdgenomics.guacamole.concordance

import org.bdgenomics.guacamole.{ Common, Command }
import org.apache.spark.{ SparkContext, Logging }
import org.kohsuke.args4j.Option
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.apache.spark.SparkContext._
import org.bdgenomics.adam.rdd.variation.ConcordanceTable
import java.util.EnumSet
import org.bdgenomics.formats.avro.{ Genotype, GenotypeType }
import org.bdgenomics.adam.rich.RichVariant
import org.apache.spark.rdd.RDD

object GenotypesEvaluator extends Command with Logging {
  val name: String = "varianteval"
  val description: String = "Evaluation script for scoring"

  trait GenotypeConcordance extends Common.Arguments.Base {

    @Option(name = "-truth", metaVar = "truth", usage = "The truth ADAM or VCF genotypes file")
    var truthGenotypesFile: String = ""

    @Option(name = "-exclude-snv", usage = "Exclude SNV variants in comparison")
    var excludeSNVs: Boolean = false
    @Option(name = "-exclude-indel", usage = "Exclude indel variants in comparison")
    var excludeIndels: Boolean = false
    @Option(name = "-chr", usage = "Chromosome to filter to")
    var chromosome: String = ""
  }

  private class Arguments extends GenotypeConcordance {

    @Option(name = "-test", required = true, usage = "The test ADAM genotypes file")
    var testGenotypesFile: String = ""
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(appName = Some(name))

    val calledGenotypes = Common.loadGenotypes(args.testGenotypesFile, sc)
    printGenotypeConcordance(args, calledGenotypes, sc)
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
  def computePrecisionAndRecall(calledGenotypes: RDD[Genotype],
                                trueGenotypes: RDD[Genotype],
                                excludeSNVs: Boolean = false,
                                excludeIndels: Boolean = true,
                                chromosome: String = null): (Double, Double, Double) = {
    val chromosomeFilter: (Genotype => Boolean) = chromosome == "" || _.getVariant.getContig.getContigName.toString == chromosome
    val variantTypeFilter: (Genotype => Boolean) = genotype => {
      val variant = new RichVariant(genotype.getVariant)
      (!excludeSNVs && variant.isSingleNucleotideVariant()) || (!excludeIndels && (variant.isInsertion() || variant.isDeletion()))
    }

    val relevantVariants: (Genotype => Boolean) = v => chromosomeFilter(v) && variantTypeFilter(v)

    val filteredCalledAlleles = calledGenotypes.filter(relevantVariants)
    val filteredTrueGenotypes = trueGenotypes.filter(relevantVariants)

    val sampleName = filteredCalledAlleles.take(1)(0).getSampleId.toString
    val sampleAccuracy = filteredCalledAlleles.concordanceWith(filteredTrueGenotypes).collectAsMap()(sampleName)

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

  def printGenotypeConcordance(args: GenotypeConcordance, genotypes: RDD[Genotype], sc: SparkContext) = {
    val trueGenotypes = Common.loadGenotypes(args.truthGenotypesFile, sc)

    val (precision, recall, f1score) = computePrecisionAndRecall(genotypes, trueGenotypes, args.excludeSNVs, args.excludeIndels, args.chromosome)
    println("Precision\tRecall\tF1Score")
    println("%f\t%f\t%f".format(precision, recall, f1score))
  }

}
