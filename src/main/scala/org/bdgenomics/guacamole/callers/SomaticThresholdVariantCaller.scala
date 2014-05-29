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
import org.bdgenomics.adam.avro.ADAMGenotypeAllele.{ NoCall, Ref, Alt, OtherAlt }
import org.bdgenomics.guacamole._
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions
import org.kohsuke.args4j.Option
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.Common.Arguments._
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.bdgenomics.guacamole.pileup.Pileup

/**
 * Simple somatic variant caller.
 *
 * Calls variants when the percent of reads supporting a variant is both:
 *    - LESS than a threshold ("threshold-normal") in the normal sample, AND
 *    - MORE than a threshold ("threshold-tumor") in the tumor sample.
 *
 */
object  SomaticThresholdVariantCaller extends Command with Serializable with Logging {
  override val name = "somatic-threshold"
  override val description = "call somatic variants using a two-threshold criterion"

  private class Arguments extends Base with Output with TumorNormalReads with DistributedUtil.Arguments {
    @Option(name = "-threshold-normal", metaVar = "X",
      usage = "Somatic variants must have fewer than X% of reads in the normal sample.")
    var thresholdNormal: Int = 4

    @Option(name = "-threshold-tumor", metaVar = "X",
      usage = "Somatic variants must have more than X% of reads in the tumor sample.")
    var thresholdTumor: Int = 10
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    val (rawTumorReads, tumorDictionary, rawNormalReads, normalDictionary) =
      Common.loadTumorNormalReadsFromArguments(args, sc, mapped = true, nonDuplicate = true)

    assert(tumorDictionary == normalDictionary,
      "Tumor and normal samples have different sequence dictionaries. Tumor dictionary: %s.\nNormal dictionary: %s."
        .format(tumorDictionary, normalDictionary))

    val mappedTumorReads = rawTumorReads.map(_.getMappedRead).filter(_.mdTag.isDefined)
    val mappedNormalReads = rawNormalReads.map(_.getMappedRead).filter(_.mdTag.isDefined)

    mappedTumorReads.persist()
    mappedNormalReads.persist()

    Common.progress("Loaded %,d tumor mapped non-duplicate MdTag-containing reads into %,d partitions.".format(
      mappedTumorReads.count, mappedTumorReads.partitions.length))
    Common.progress("Loaded %,d normal mapped non-duplicate MdTag-containing reads into %,d partitions.".format(
      mappedNormalReads.count, mappedNormalReads.partitions.length))

    val loci = Common.loci(args, normalDictionary)
    val (thresholdNormal, thresholdTumor) = (args.thresholdNormal, args.thresholdTumor)
    val numGenotypes = sc.accumulator(0L)
    DelayedMessages.default.say { () => "Called %,d genotypes.".format(numGenotypes.value) }
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, mappedTumorReads, mappedNormalReads)
    val genotypes: RDD[ADAMGenotype] = DistributedUtil.pileupFlatMapTwoRDDs[ADAMGenotype](
      mappedTumorReads,
      mappedNormalReads,
      lociPartitions,
      (pileupTumor, pileupNormal) => {
        val genotypes = callVariantsAtLocus(pileupTumor, pileupNormal, thresholdTumor, thresholdNormal)
        numGenotypes += genotypes.length
        genotypes.iterator
      })
    mappedTumorReads.unpersist()
    mappedNormalReads.unpersist()
    Common.writeVariantsFromArguments(args, genotypes)
    DelayedMessages.default.print()
  }

  def callVariantsAtLocus(
    pileupTumor: Pileup,
    pileupNormal: Pileup,
    thresholdTumor: Int,
    thresholdNormal: Int): Seq[ADAMGenotype] = {

    // We skip loci where either tumor or normal samples have no reads mapped.
    if (pileupTumor.elements.isEmpty || pileupNormal.elements.isEmpty)
      return Seq.empty

    val refBase = pileupTumor.referenceBase
    assert(refBase == pileupNormal.referenceBase)

    // Given a Pileup, return a Map from single base alleles to the percent of reads that have that allele.
    def possibleSNVAllelePercents(pileup: Pileup): Map[Byte, Double] = {
      val totalReads = pileup.elements.length
      val matchesOrMismatches = pileup.elements.filter(e => e.isMatch || e.isMismatch)
      val counts = matchesOrMismatches.map(_.sequencedSingleBase).groupBy(char => char).mapValues(_.length)
      val percents = counts.mapValues(_ * 100.0 / totalReads.toDouble)
      percents.withDefaultValue(0.0)
    }

    def variant(alternateBase: Byte, allelesList: List[ADAMGenotypeAllele]): ADAMGenotype = {
      ADAMGenotype.newBuilder
        .setAlleles(JavaConversions.seqAsJavaList(allelesList))
        .setSampleId("somatic".toCharArray)
        .setVariant(ADAMVariant.newBuilder
          .setPosition(pileupNormal.locus)
          .setReferenceAllele(Bases.baseToString(pileupNormal.referenceBase))
          .setVariantAllele(Bases.baseToString(alternateBase))
          .setContig(ADAMContig.newBuilder.setContigName(pileupNormal.referenceName).build)
          .build)
        .build
    }

    val possibleAllelesTumor = possibleSNVAllelePercents(pileupTumor)
    val possibleAllelesNormal = possibleSNVAllelePercents(pileupNormal)
    val possibleAlleles = possibleAllelesTumor.keySet.union(possibleAllelesNormal.keySet).toSeq.sortBy(-1 * possibleAllelesTumor(_))

    val thresholdSatisfyingAlleles = possibleAlleles.filter(
      base => possibleAllelesNormal(base) < thresholdNormal && possibleAllelesTumor(base) > thresholdTumor)

    // For now, we call a het when we have one somatic variant, and a compound alt when we have two. This is not really
    // correct though. We should take into account the evidence for the reference allele in the tumor, and call het or
    // hom alt accordingly.
    thresholdSatisfyingAlleles.toList match {
      case Nil         => Nil
      case base :: Nil => variant(base, Alt :: Ref :: Nil) :: Nil
      case base1 :: base2 :: rest =>
        variant(base1, Alt :: OtherAlt :: Nil) :: variant(base2, Alt :: OtherAlt :: Nil) :: Nil
    }
  }
}