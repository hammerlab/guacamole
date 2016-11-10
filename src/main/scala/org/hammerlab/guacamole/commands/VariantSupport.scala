package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.Variant
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapMultipleSamples
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.readsets.args.{ReferenceArgs, Arguments => ReadSetsArguments}
import org.hammerlab.guacamole.readsets.io.InputConfig
import org.hammerlab.guacamole.readsets.rdd.{PartitionedRegions, PartitionedRegionsArgs}
import org.hammerlab.guacamole.readsets.{PerSample, ReadSets, SampleName}
import org.hammerlab.guacamole.reference.{ContigName, Locus}
import org.hammerlab.guacamole.util.Bases.basesToString
import org.kohsuke.args4j.{Option => Args4jOption}

object VariantSupport {

  protected class Arguments
    extends Args
      with ReadSetsArguments
      with PartitionedRegionsArgs
      with ReferenceArgs {

    @Args4jOption(name = "--input-variant", required = true, aliases = Array("-v"),
      usage = "")
    var variants: String = ""

    @Args4jOption(name = "--output", metaVar = "OUT", required = true, aliases = Array("-o"),
      usage = "Output path for CSV")
    var output: String = ""
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

      val reference = args.reference(sc)

      val adamContext = new ADAMContext(sc)

      val variants: RDD[Variant] = adamContext.loadVariants(args.variants).rdd

      val readsets =
        ReadSets(
          sc,
          args.inputs,
          InputConfig.empty
        )

      // Build a loci set from the variant positions
      val loci =
        LociSet(
          variants
            .map(variant => (variant.getContigName, variant.getStart: Long, variant.getEnd: Long))
            .collect()
        )

      val partitionedReads =
        PartitionedRegions(
          readsets.allMappedReads,
          loci,
          args
        )

      val alleleCounts =
        pileupFlatMapMultipleSamples[AlleleCount](
          readsets.sampleNames,
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
          basesToString(allele.refBases),
          basesToString(allele.altBases),
          elements.size
        )
  }
}
