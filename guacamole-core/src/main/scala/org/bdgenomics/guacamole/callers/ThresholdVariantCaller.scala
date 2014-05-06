/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.guacamole.callers

import org.bdgenomics.adam.avro.{ ADAMContig, ADAMVariant, ADAMGenotypeAllele, ADAMGenotype }
import org.bdgenomics.adam.avro.ADAMGenotypeAllele.{ NoCall, Ref, Alt }
import org.bdgenomics.guacamole._
import scala.collection.JavaConversions
import org.kohsuke.args4j.Option
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.Common.Arguments._
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging

/**
 * Simple variant caller implementation.
 *
 * Instead of a bayesian approach, just uses thresholds on read counts to call variants (similar to Varscan).
 *
 */
object ThresholdVariantCaller extends Command with Serializable with Logging {
  override val name = "threshold"
  override val description = "call variants using a simple threshold"

  private class Arguments extends Base with Output with Reads with DistributedUtil.Arguments {
    @Option(name = "-threshold", metaVar = "X", usage = "Make a call if at least X% of reads support it. Default: 8")
    var threshold: Int = 8
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args)

    val threshold = args.threshold
    val reads = Common.loadReads(args, sc, mapped = true, nonDuplicate = true)
    val loci = Common.loci(args, reads)
    val genotypes: RDD[ADAMGenotype] = DistributedUtil.pileupFlatMap[ADAMGenotype](
      reads,
      loci,
      args.parallelism,
      callVariantsAtLocus(threshold, _))
    Common.writeVariants(args, genotypes)
  }

  def callVariantsAtLocus(threshold_percent: Int, pileup: Pileup): Seq[ADAMGenotype] = {
    // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
    if (pileup.elements.isEmpty) {
      log.warn("Skipping empty pileup at locus: %d".format(pileup.locus))
      return Seq.empty
    }

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