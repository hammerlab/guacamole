package org.bdgenomics.guacamole.callers

import org.bdgenomics.adam.avro.{ ADAMContig, ADAMVariant, ADAMGenotypeAllele, ADAMGenotype }
import org.bdgenomics.adam.avro.ADAMGenotypeAllele.{ NoCall, Ref, Alt }
import org.bdgenomics.guacamole._
import scala.collection.immutable.NumericRange
import scala.collection.JavaConversions
import org.kohsuke.args4j.{ Option, Argument }
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.SlidingReadWindow
import org.bdgenomics.guacamole.Common.Arguments._
import org.bdgenomics.guacamole.SlidingReadWindow

/**
 * Simple variant caller implementation.
 *
 * Instead of a bayesian approach, just uses thresholds on read counts to call variants (similar to Varscan).
 *
 */
class ThresholdVariantCaller(threshold_percent: Int) extends PileupVariantCaller with Serializable {
  def callVariantsAtLocus(pileup: Pileup): Seq[ADAMGenotype] = {
    val refBase = pileup.referenceBase
    pileup.bySample.toSeq.flatMap({
      case (sampleName, samplePileup) =>
        val totalReads = samplePileup.elements.length
        val matchesOrMismatches = samplePileup.elements.filter(e => e.isMatch || e.isMismatch)
        val counts = matchesOrMismatches.map(_.singleBaseRead).groupBy(char => char).mapValues(_.length)
        val sortedAlleles = counts.toList.filter(_._2 * 100 / totalReads > threshold_percent).sortBy(-1 * _._2)

        def variant(alternateBase: Char, allelesList: List[ADAMGenotypeAllele]): ADAMGenotype = {
          ADAMGenotype.newBuilder
            .setAlleles(JavaConversions.seqAsJavaList(allelesList))
            .setSampleId(sampleName.toCharArray)
            .setVariant(ADAMVariant.newBuilder
              .setPosition(pileup.locus)
              .setReferenceAllele(pileup.referenceBase.toString)
              .setVariantAllele(alternateBase.toString.toCharArray)
              .setContig(ADAMContig.newBuilder.setContigName(pileup.referenceName).build)
              .build)
            .build
        }

        sortedAlleles match {
          /* If no alleles are above our threshold, we emit a NoCall variant with the reference allele
           * as the variant allele.
           */
          case Nil =>
            variant(refBase, NoCall :: NoCall :: Nil) :: Nil

          // Hom Ref.
          case (base, count) :: Nil if base == refBase =>
            variant(refBase, Ref :: Ref :: Nil) :: Nil

          // Hom alt.
          case (base: Char, count) :: Nil =>
            variant(base, Alt :: Alt :: Nil) :: Nil

          // Het alt.
          case (base1, count1) :: (base2, count2) :: rest if base1 == refBase || base2 == refBase =>
            variant(if (base1 != refBase) base1 else base2, Ref :: Alt :: Nil) :: Nil

          // Compound alt
          // TODO: ADAM needs to have an "OtherAlt" allele for this case!
          case (base1, count1) :: (base2, count2) :: rest =>
            variant(base1, Alt :: Alt :: Nil) :: variant(base2, Alt :: Alt :: Nil) :: Nil
        }
    })
  }
}
object ThresholdVariantCaller extends Command {
  override val name = "threshold"
  override val description = "call variants using a simple threshold"

  private class Arguments extends Base with Output with Reads with SlidingWindowVariantCaller.Arguments {
    @Option(name = "-threshold", metaVar = "X", usage = "Make a call if at least X% of reads support it. Default: 8")
    var threshold: Int = 8
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args)

    val reads = Common.loadReads(args, sc, mapped = true, nonDuplicate = true)
    val caller = new ThresholdVariantCaller(args.threshold)
    SlidingWindowVariantCaller.invoke(args, caller, reads)
  }
}