package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.Variant
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapMultipleSamples
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.readsets.args.{Arguments => ReadSetsArguments}
import org.hammerlab.guacamole.readsets.io.{InputFilters, ReadLoadingConfig}
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegions
import org.hammerlab.guacamole.readsets.{PerSample, ReadSets, SampleName}
import org.hammerlab.guacamole.reference.{ContigName, Locus, ReferenceBroadcast}
import org.hammerlab.guacamole.util.Bases
import org.kohsuke.args4j.{Option => Args4jOption}

object VariantSupport {

  protected class Arguments extends ReadSetsArguments {
    @Args4jOption(name = "--input-variant", required = true, aliases = Array("-v"),
      usage = "")
    var variants: String = ""

    @Args4jOption(name = "--output", metaVar = "OUT", required = true, aliases = Array("-o"),
      usage = "Output path for CSV")
    var output: String = ""

    @Args4jOption(name = "--reference-fasta", required = true, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = ""

  }

  object Caller extends SparkCommand[Arguments] {

    override val name = "variant-support"
    override val description = "Find number of reads that support each variant across BAMs"

    case class AlleleCount(sampleName: SampleName,
                           contigName: ContigName,
                           locus: Locus,
                           reference: String,
                           alternate: String,
                           count: Int) {
      override def toString: String = {
        s"$sampleName, $contigName, $locus, $reference, $alternate, $count"
      }
    }

    override def run(args: Arguments, sc: SparkContext): Unit = {

      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)

      val adamContext = new ADAMContext(sc)

      val variants: RDD[Variant] = adamContext.loadVariants(args.variants)

      val readsets =
        ReadSets(
          sc,
          args.inputs,
          InputFilters.empty,
          config = ReadLoadingConfig(args)
        )

      // Build a loci set from the variant positions
      val loci =
        LociSet(
          variants
            .map(variant => (variant.getContig.getContigName, variant.getStart: Long, variant.getEnd: Long))
            .collect()
        )

      val partitionedReads =
        PartitionedRegions(
          readsets.allMappedReads,
          loci,
          args,
          halfWindowSize = 0
        )

      val alleleCounts =
        pileupFlatMapMultipleSamples[AlleleCount](
          readsets.numSamples,
          partitionedReads,
          skipEmpty = true,
          pileupsToAlleleCounts,
          reference
        )

      alleleCounts.saveAsTextFile(args.output)
    }

    /**
     * Count alleles in a pileup
     *
     * @param pileups Per-sample pileups of reads at a given locus.
     * @return Iterator of AlleleCount which contains pair of reference and alternate with a count.
     */
    def pileupsToAlleleCounts(pileups: PerSample[Pileup]): Iterator[AlleleCount] =
      for {
        pileup <- pileups.iterator
        (allele, elements) <- pileup.elements.groupBy(_.allele)
      } yield
        AlleleCount(
          pileup.sampleName,
          pileup.contigName,
          pileup.locus,
          Bases.basesToString(allele.refBases),
          Bases.basesToString(allele.altBases),
          elements.size
        )
  }
}
