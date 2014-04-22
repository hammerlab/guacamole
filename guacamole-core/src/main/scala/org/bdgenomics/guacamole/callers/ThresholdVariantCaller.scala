package org.bdgenomics.guacamole.callers

import org.bdgenomics.adam.avro.{ ADAMContig, ADAMVariant, ADAMGenotypeAllele, ADAMGenotype }
import org.bdgenomics.adam.avro.ADAMGenotypeAllele.{ NoCall, Ref, Alt }
import org.bdgenomics.guacamole._
import scala.collection.immutable.NumericRange
import scala.collection.JavaConversions
import org.kohsuke.args4j.{ Option, Argument }
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.SlidingReadWindow

/**
 * Example variant caller implementation.
 *
 * Instead of a bayesian approach, just uses thresholds on read counts to call variants (similar to Varscan).
 *
 */
class ThresholdVariantCaller(threshold_percent: Int) extends VariantCaller {

  override val halfWindowSize: Long = 0

  override def callVariants(samples: Seq[String], reads: SlidingReadWindow, loci: LociSet.SingleContig): Iterator[ADAMGenotype] = {
    val lociAndReads = loci.individually.map(locus => (locus, reads.setCurrentLocus(locus)))
    val pileupsIterator = Pileup.pileupsAtLoci(lociAndReads)
    pileupsIterator.flatMap(callVariantsAtLocus _)
  }

  private def callVariantsAtLocus(pileup: Pileup): Seq[ADAMGenotype] = {
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
object ThresholdVariantCaller extends VariantCallerFactory {
  val name = "threshold"
  val description = "call variants using a simple threshold"

  override def fromCommandLineArguments(args: Array[String]): (GuacamoleCommonArguments, ThresholdVariantCaller) = {
    class Arguments extends GuacamoleCommonArguments {
      @Option(name = "-threshold", metaVar = "X (percent)", usage = "Make a call if at least X percent of reads support it")
      var threshold: Int = 8
    }
    val parsedArgs = Args4j[Arguments](args)
    val instance = new ThresholdVariantCaller(parsedArgs.threshold)
    (parsedArgs, instance)
  }
}