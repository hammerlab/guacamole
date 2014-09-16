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

import org.bdgenomics.formats.avro.{ Contig, Variant, GenotypeAllele, Genotype }
import org.bdgenomics.formats.avro.GenotypeAllele.{ NoCall, Ref, Alt, OtherAlt }
import org.bdgenomics.guacamole._
import org.apache.spark.SparkContext._
import org.bdgenomics.guacamole.reads.Read
import scala.collection.JavaConversions
import org.kohsuke.args4j.Option
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.Common.Arguments._
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.bdgenomics.guacamole.pileup.{ Allele, Pileup }
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator.GenotypeConcordance

/**
 * Simple variant caller.
 *
 * Calls variants when the percent of reads supporting a variant at a locus exceeds a threshold.
 *
 */
object ThresholdVariantCaller extends Command with Serializable with Logging {
  override val name = "threshold"
  override val description = "call variants using a simple threshold"

  private class Arguments extends Base with Output with Reads with GenotypeConcordance with DistributedUtil.Arguments {
    @Option(name = "-threshold", metaVar = "X", usage = "Make a call if at least X% of reads support it. Default: 8")
    var threshold: Int = 8

    @Option(name = "-emit-ref", usage = "Output homozygous reference calls.")
    var emitRef: Boolean = false

    @Option(name = "-emit-no-call", usage = "Output no call calls.")
    var emitNoCall: Boolean = false
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(appName = Some(name))

    val readSet = Common.loadReadsFromArguments(args, sc, Read.InputFilters(mapped = true, nonDuplicate = true))

    readSet.mappedReads.persist()
    Common.progress("Loaded %,d mapped non-duplicate MdTag-containing reads into %,d partitions.".format(
      readSet.mappedReads.count, readSet.mappedReads.partitions.length))

    val loci = Common.loci(args, readSet)
    val (threshold, emitRef, emitNoCall) = (args.threshold, args.emitRef, args.emitNoCall)
    val numGenotypes = sc.accumulator(0L)
    DelayedMessages.default.say { () => "Called %,d genotypes.".format(numGenotypes.value) }
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, readSet.mappedReads)
    val genotypes: RDD[Genotype] = DistributedUtil.pileupFlatMap[Genotype](
      readSet.mappedReads,
      lociPartitions,
      true, // skip empty pileups
      pileup => {
        val genotypes = callVariantsAtLocus(pileup, threshold, emitRef, emitNoCall)
        numGenotypes += genotypes.length
        genotypes.iterator
      })
    readSet.mappedReads.unpersist()
    Common.writeVariantsFromArguments(args, genotypes)
    if (args.truthGenotypesFile != "")
      GenotypesEvaluator.printGenotypeConcordance(args, genotypes, sc)

    DelayedMessages.default.print()
  }

  def callVariantsAtLocus(
    pileup: Pileup,
    thresholdPercent: Int,
    emitRef: Boolean = true,
    emitNoCall: Boolean = true): Seq[Genotype] = {

    // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
    if (pileup.elements.isEmpty)
      return Seq.empty

    pileup.bySample.toSeq.flatMap({
      case (sampleName, samplePileup) =>
        val totalReads = samplePileup.elements.length
        val counts = samplePileup.elements.map(_.allele).groupBy(x => x).mapValues(_.length)
        val sortedAlleles = counts.toList.filter(_._2 * 100 / totalReads > thresholdPercent).sortBy(-1 * _._2)

        def variant(allele: Allele, allelesList: List[GenotypeAllele]): Genotype = {
          Genotype.newBuilder
            .setAlleles(JavaConversions.seqAsJavaList(allelesList))
            .setSampleId(sampleName.toCharArray)
            .setVariant(Variant.newBuilder
              .setStart(pileup.locus)
              .setReferenceAllele(Bases.basesToString(allele.refBases))
              .setAlternateAllele(Bases.basesToString(allele.altBases))
              .setContig(Contig.newBuilder.setContigName(pileup.referenceName).build)
              .build)
            .build
        }

        sortedAlleles match {
          /* If no alleles are above our threshold, we emit a NoCall variant with the reference allele
           * as the variant allele.
           */
          case Nil =>
            if (emitNoCall)
              variant(
                Allele(
                  Seq(pileup.referenceBase),
                  Seq(pileup.referenceBase)
                ),
                NoCall :: NoCall :: Nil
              ) :: Nil
            else
              Nil

          // Hom Ref.
          case (allele, count) :: Nil if !allele.isVariant =>
            if (emitRef) (variant(allele, Ref :: Ref :: Nil) :: Nil) else Nil

          // Hom alt.
          case (allele: Allele, count) :: Nil =>
            variant(allele, Alt :: Alt :: Nil) :: Nil

          // Het alt.
          case (allele1, count1) :: (allele2, count2) :: rest if !allele1.isVariant || !allele2.isVariant =>
            variant(if (allele1.isVariant) allele1 else allele2, Ref :: Alt :: Nil) :: Nil

          // Compound alt
          case (allele1, count1) :: (allele2, count2) :: rest =>
            variant(allele1, Alt :: OtherAlt :: Nil) :: variant(allele2, Alt :: OtherAlt :: Nil) :: Nil
        }
    })
  }
}
