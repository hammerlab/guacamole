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
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapMultipleRDDs
import org.hammerlab.guacamole.loci.partitioning.LociPartitionerArgs
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.readsets.io.{InputFilters, ReadLoadingConfig, ReadLoadingConfigArgs}
import org.hammerlab.guacamole.readsets.{PerSample, ReadSets, SampleName}
import org.hammerlab.guacamole.reference.{ContigName, Locus, ReferenceBroadcast}
import org.hammerlab.guacamole.util.Bases
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

object VariantSupport {

  protected class Arguments extends LociPartitionerArgs with ReadLoadingConfigArgs {
    @Args4jOption(name = "--input-variant", required = true, aliases = Array("-v"),
      usage = "")
    var variants: String = ""

    @Args4jOption(name = "--output", metaVar = "OUT", required = true, aliases = Array("-o"),
      usage = "Output path for CSV")
    var output: String = ""

    @Argument(required = true, multiValued = true,
      usage = "Retrieve read data from BAMs at each variant position")
    var bams: Array[String] = Array.empty

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
          args.bams,
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

      val lociPartitions =
        args
          .getPartitioner(readsets.allMappedReads)
          .partition(loci)


      val alleleCounts =
        pileupFlatMapMultipleRDDs[AlleleCount](
          readsets.mappedReadsRDDs,
          lociPartitions,
          skipEmpty = true,
          pileupToAlleleCounts,
          reference
        )

      alleleCounts.saveAsTextFile(args.output)
    }

    /**
     * Count alleles in a pileup
     *
     * @param pileups Per-sample pileups of reads a given locus.
     * @return Iterator of AlleleCount which contains pair of reference and alternate with a count.
     */
    def pileupToAlleleCounts(pileups: PerSample[Pileup]): Iterator[AlleleCount] =
      (for {
        pileup <- pileups
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
      ).iterator
  }
}
