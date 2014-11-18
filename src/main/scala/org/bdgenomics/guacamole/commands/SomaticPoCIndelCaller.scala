/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.guacamole.commands

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.{ Contig, Variant, GenotypeAllele, Genotype }
import org.bdgenomics.guacamole.Common.Arguments.SomaticCallerArgs
import org.bdgenomics.guacamole.{ SparkCommand, Bases, DelayedMessages, Common, DistributedUtil }
import org.bdgenomics.guacamole.pileup.{ Insertion, Deletion, Pileup }
import org.bdgenomics.guacamole.reads.Read
import scala.collection.JavaConversions

object SomaticPoCIndel {

  protected class Arguments extends SomaticCallerArgs

  object Caller extends SparkCommand[Arguments] {

    override val name = "somatic-poc"
    override val description = "call simple insertion and deletion variants between a tumor and a normal"

    override def run(args: Arguments, sc: SparkContext): Unit = {

      val filters = Read.InputFilters(mapped = true, nonDuplicate = true, passedVendorQualityChecks = true)
      val (tumorReads, normalReads) = Common.loadTumorNormalReadsFromArguments(args, sc, filters)

      assert(tumorReads.sequenceDictionary == normalReads.sequenceDictionary,
        "Tumor and normal samples have different sequence dictionaries. Tumor dictionary: %s.\nNormal dictionary: %s."
          .format(tumorReads.sequenceDictionary, normalReads.sequenceDictionary))

      val loci = Common.loci(args, normalReads)
      val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, tumorReads.mappedReads, normalReads.mappedReads)

      val genotypes: RDD[Genotype] = DistributedUtil.pileupFlatMapTwoRDDs[Genotype](
        tumorReads.mappedReads,
        normalReads.mappedReads,
        lociPartitions,
        skipEmpty = true, // skip empty pileups
        (pileupTumor, pileupNormal) => callSimpleIndelsAtLocus(
          pileupTumor,
          pileupNormal
        ).iterator
      )

      Common.writeVariantsFromArguments(args, genotypes)
      DelayedMessages.default.print()
    }

    def callSimpleIndelsAtLocus(pileupTumor: Pileup, pileupNormal: Pileup): Seq[Genotype] = {
      val tumorDeletions = pileupTumor.elements.map(_.alignment).collect { case d: Deletion => d }
      val normalDeletions = pileupNormal.elements.map(_.alignment).collect { case d: Deletion => d }

      // As a PoC here I'm just emitting a deletion if more tumor reads showed a deletion than normal reads did.
      val deletions =
        if (tumorDeletions.size > normalDeletions.size) {
          val tumorDeletion = tumorDeletions.head
          Seq(
            Genotype
              .newBuilder()
              .setAlleles(JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
              .setSampleId("somatic")
              .setVariant(
                Variant
                  .newBuilder()
                  .setContig(
                    Contig.newBuilder.setContigName(pileupNormal.referenceName).build
                  )
                  .setStart(pileupNormal.locus)
                  .setReferenceAllele(Bases.basesToString(tumorDeletion.referenceBases))
                  .setAlternateAllele(Bases.baseToString(tumorDeletion.referenceBases(0)))
                  .build
              )
              .build
          )
        } else {
          Nil
        }

      val tumorInsertions = pileupTumor.elements.map(_.alignment).collect { case d: Insertion => d }
      val normalInsertions = pileupNormal.elements.map(_.alignment).collect { case d: Insertion => d }

      val insertions =
        if (tumorInsertions.size > normalInsertions.size) {
          val tumorInsertion = tumorInsertions.head
          Seq(
            Genotype
              .newBuilder()
              .setAlleles(JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
              .setSampleId("somatic")
              .setVariant(
                Variant
                  .newBuilder()
                  .setContig(
                    Contig.newBuilder.setContigName(pileupNormal.referenceName).build
                  )
                  .setStart(pileupNormal.locus)
                  .setReferenceAllele(Bases.basesToString(tumorInsertion.referenceBases))
                  .setAlternateAllele(Bases.basesToString(tumorInsertion.sequencedBases))
                  .build
              )
              .build
          )
        } else {
          Nil
        }

      deletions ++ insertions
    }
  }
}
