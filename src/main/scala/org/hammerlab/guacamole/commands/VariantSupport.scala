package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.Variant
import org.hammerlab.args4s.PathHandler
import org.hammerlab.cli.args4j.Args
import org.hammerlab.genomics.bases.Bases
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.readsets.args.impl.{ ReferenceArgs, Arguments ⇒ ReadSetsArguments }
import org.hammerlab.genomics.readsets.io.Config
import org.hammerlab.genomics.readsets.{ PerSample, ReadSets, SampleName }
import org.hammerlab.genomics.reference.{ ContigName, Locus, Region }
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapMultipleSamples
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.readsets.rdd.{ PartitionedRegions, PartitionedRegionsArgs }
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.paths.Path
import org.kohsuke.args4j.{ Option ⇒ Args4jOption }

object VariantSupport {

  protected class Arguments
    extends Args
      with ReadSetsArguments
      with PartitionedRegionsArgs
      with ReferenceArgs {

    @Args4jOption(
      name = "--input-variants",
      aliases = Array("-v"),
      required = true,
      handler = classOf[PathHandler],
      usage = ""
    )
    var variants: Path = _

    @Args4jOption(
      name = "--output",
      aliases = Array("-o"),
      metaVar = "OUT",
      required = true,
      handler = classOf[PathHandler],
      usage = "Output path for CSV"
    )
    var output: Path = _
  }

  object Caller extends GuacCommand[Arguments] {

    override val name = "variant-support"
    override val description = "Find number of reads that support each variant across BAMs"

    case class GenotypeCount(sampleName: SampleName,
                             contigName: ContigName,
                             locus: Locus,
                             reference: Bases,
                             alternate: Bases,
                             count: Int) {
      override def toString: String =
        s"$sampleName, $contigName, $locus, $reference, $alternate, $count"
    }

    override def run(args: Arguments, sc: SparkContext): Unit = {

      val reference = ReferenceBroadcast(args, sc)

      val adamContext: ADAMContext = sc

      val variants: RDD[Variant] = adamContext.loadVariants(args.variants).rdd

      val readsets =
        ReadSets(
          sc,
          args.inputs,
          Config.empty(sc.hadoopConfiguration)
        )

      // Build a loci set from the variant positions
      val loci =
        LociSet(
          variants
            .map(variant ⇒
              Region(
                variant.getContigName,
                Locus(variant.getStart),
                Locus(variant.getEnd)
              )
            )
            .collect()
        )

      val partitionedReads =
        PartitionedRegions(
          readsets.sampleIdxKeyedMappedReads,
          loci,
          args
        )

      val alleleCounts =
        pileupFlatMapMultipleSamples[GenotypeCount](
          readsets.sampleNames,
          partitionedReads,
          skipEmpty = true,
          pileupsToAlleleCounts,
          reference
        )

      alleleCounts.saveAsTextFile(args.output.toString)
    }

    /**
     * Count alleles in a pileup
     *
     * @param pileups Per-sample pileups of reads at a given locus.
     * @return Iterator of AlleleCount which contains pair of reference and alternate with a count.
     */
    def pileupsToAlleleCounts(pileups: PerSample[Pileup]): Iterator[GenotypeCount] =
      for {
        pileup ← pileups.iterator
        (allele, elements) ← pileup.elements.groupBy(_.allele)
      } yield
        GenotypeCount(
          pileup.sampleName,
          pileup.contigName,
          pileup.locus,
          allele.refBases,
          allele.altBases,
          elements.size
        )
  }
}
