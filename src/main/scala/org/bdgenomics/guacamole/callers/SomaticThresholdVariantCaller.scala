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
import org.bdgenomics.formats.avro.GenotypeAllele.{ Ref, Alt, OtherAlt }
import org.bdgenomics.guacamole._
import org.apache.spark.SparkContext._
import org.bdgenomics.guacamole.reads.Read
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
object SomaticThresholdVariantCaller extends Command with Serializable with Logging {
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
    val sc = Common.createSparkContext(appName = Some(name))

    val filters = Read.InputFilters(mapped = true, nonDuplicate = true)
    val (tumorReads, normalReads) = Common.loadTumorNormalReadsFromArguments(args, sc, filters)

    assert(tumorReads.sequenceDictionary == normalReads.sequenceDictionary,
      "Tumor and normal samples have different sequence dictionaries. Tumor dictionary: %s.\nNormal dictionary: %s."
        .format(tumorReads.sequenceDictionary, normalReads.sequenceDictionary))

    tumorReads.mappedReads.persist()
    normalReads.mappedReads.persist()

    Common.progress("Loaded %,d tumor mapped non-duplicate MdTag-containing reads into %,d partitions.".format(
      tumorReads.mappedReads.count, tumorReads.mappedReads.partitions.length))
    Common.progress("Loaded %,d normal mapped non-duplicate MdTag-containing reads into %,d partitions.".format(
      normalReads.mappedReads.count, normalReads.mappedReads.partitions.length))

    val loci = Common.loci(args, normalReads)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, tumorReads.mappedReads, normalReads.mappedReads)

    val (thresholdNormal, thresholdTumor) = (args.thresholdNormal, args.thresholdTumor)
    val numGenotypes = sc.accumulator(0L)
    DelayedMessages.default.say { () => "Called %,d genotypes.".format(numGenotypes.value) }

    val genotypes: RDD[Genotype] = DistributedUtil.pileupFlatMapTwoRDDs[Genotype](
      tumorReads.mappedReads,
      normalReads.mappedReads,
      lociPartitions,
      skipEmpty = true, // skip empty pileups
      (pileupTumor, pileupNormal) => {
        val genotypes = callVariantsAtLocus(pileupTumor, pileupNormal, thresholdTumor, thresholdNormal)
        numGenotypes += genotypes.length
        genotypes.iterator
      })
    tumorReads.mappedReads.unpersist()
    normalReads.mappedReads.unpersist()
    Common.writeVariantsFromArguments(args, genotypes)
    DelayedMessages.default.print()
  }

  def callVariantsAtLocus(
    pileupTumor: Pileup,
    pileupNormal: Pileup,
    thresholdTumor: Int,
    thresholdNormal: Int): Seq[Genotype] = {

    // We skip loci where either tumor or normal samples have no reads mapped.
    if (pileupTumor.elements.isEmpty || pileupNormal.elements.isEmpty)
      return Seq.empty

    assert(pileupTumor.referenceBase == pileupNormal.referenceBase)
    val refBases = Seq(pileupTumor.referenceBase)

    // Given a Pileup, return a Map from single base alleles to the percent of reads that have that allele.
    def possibleSNVAllelePercents(pileup: Pileup): Map[Seq[Byte], Double] = {
      val totalReads = pileup.elements.length
      val matchesOrMismatches = pileup.elements.filter(e => e.isMatch || e.isMismatch)
      val counts = matchesOrMismatches.map(_.sequencedBases).groupBy(char => char).mapValues(_.length)
      val percents = counts.mapValues(_ * 100.0 / totalReads.toDouble)
      percents.withDefaultValue(0.0)
    }

    def variant(alternateBases: Seq[Byte], allelesList: List[GenotypeAllele]): Genotype = {
      Genotype.newBuilder
        .setAlleles(JavaConversions.seqAsJavaList(allelesList))
        .setSampleId("somatic".toCharArray)
        .setVariant(Variant.newBuilder
          .setStart(pileupNormal.locus)
          .setReferenceAllele(Bases.baseToString(pileupNormal.referenceBase))
          .setAlternateAllele(Bases.basesToString(alternateBases))
          .setContig(Contig.newBuilder.setContigName(pileupNormal.referenceName).build)
          .build)
        .build
    }

    val possibleAllelesTumor = possibleSNVAllelePercents(pileupTumor)
    val possibleAllelesNormal = possibleSNVAllelePercents(pileupNormal)
    val possibleAlleles =
      possibleAllelesTumor
        .keySet
        .union(possibleAllelesNormal.keySet)
        .toSeq
        .sortBy(possibleAllelesTumor(_))
        .reverse

    // A variant allele is, by definition, not equal to the reference base. Filter non-variants out.
    val possibleVariantAlles = possibleAlleles.filter(_ != refBases)

    val thresholdSatisfyingAlleles = possibleVariantAlles.filter(
      base => possibleAllelesNormal(base) < thresholdNormal && possibleAllelesTumor(base) > thresholdTumor
    )

    // For now, we call a het when we have one somatic variant, and a compound alt when we have two. This is not really
    // correct though. We should take into account the evidence for the reference allele in the tumor, and call het or
    // hom alt accordingly.
    thresholdSatisfyingAlleles.toList match {
      case Nil         => Nil
      case base :: Nil => variant(base, Alt :: Ref :: Nil) :: Nil
      case base1 :: base2 :: rest => {
        variant(base1, Alt :: OtherAlt :: Nil) :: variant(base2, Alt :: OtherAlt :: Nil) :: Nil
      }
    }
  }
}
