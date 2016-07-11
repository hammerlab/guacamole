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

package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.Variant
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMap
import org.hammerlab.guacamole.loci.partitioning.{AllLociPartitionerArgs, UniformPartitioner, UniformPartitionerArgs}
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.loading.{InputFilters, ReadLoadingConfig, ReadLoadingConfigArgs}
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegions
import org.hammerlab.guacamole.readsets.{NumSamples, ReadSets, SampleId}
import org.hammerlab.guacamole.reference.{Contig, ReferenceBroadcast}
import org.hammerlab.guacamole.util.Bases
import org.kohsuke.args4j.{Option => Args4jOption}

object VariantSupport {

  protected class Arguments
    extends AllLociPartitionerArgs
      with ReadLoadingConfigArgs
      with ReadSets.Arguments {
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

    case class AlleleCount(sampleName: String,
                           contig: String,
                           locus: Long,
                           reference: String,
                           alternate: String,
                           count: Int) {
      override def toString: String = {
        s"$sampleName, $contig, $locus, $reference, $alternate, $count"
      }
    }

    override def run(args: Arguments, sc: SparkContext): Unit = {

      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)

      val adamContext = new ADAMContext(sc)

      val variants: RDD[Variant] = adamContext.loadVariants(args.variants)

      val readsets =
        ReadSets(
          sc,
          args.pathsAndSampleNames,
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
        pileupFlatMap[AlleleCount](
          partitionedReads,
          skipEmpty = true,
          pileupToAlleleCounts,
          reference = reference
        )

      alleleCounts.saveAsTextFile(args.output)
    }

    /**
     * Count alleles in a pileup
     *
     * @param pileup Pileup of reads a given locu
     * @return Iterator of AlleleCount which contains pair of reference and alternate with a count
     */
    def pileupToAlleleCounts(pileup: Pileup): Iterator[AlleleCount] =
      (for {
        (sampleId, samplePileup) <- pileup.bySampleMap
        (allele, elements) <- samplePileup.elements.groupBy(_.allele)
      } yield
        AlleleCount(
          pileup.sampleName,
          pileup.contig,
          pileup.locus,
          Bases.basesToString(allele.refBases),
          Bases.basesToString(allele.altBases),
          elements.size
        )
      ).iterator
  }
}
