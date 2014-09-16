package org.bdgenomics.guacamole.callers

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.formats.avro.{ Contig, Variant, GenotypeAllele, Genotype }
import org.bdgenomics.guacamole.Common.Arguments.{ TumorNormalReads, Output }
import org.bdgenomics.guacamole.pileup.{ Insertion, Deletion, Pileup }
import org.bdgenomics.guacamole._
import org.bdgenomics.guacamole.reads.Read

import scala.collection.JavaConversions

object IndelPoCCaller extends Command with Serializable with Logging {

  override val name = "indel-poc"
  override val description = "call simple insertion and deletion variants between a tumor and a normal"

  private class Arguments
      extends DistributedUtil.Arguments
      with Output
      with TumorNormalReads {

  }

  override def run(rawArgs: Array[String]): Unit = {

    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

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
