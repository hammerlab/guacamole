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
import org.hammerlab.guacamole.loci.LociSet
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{MappedRead, ReadInputFilters, ReadLoadingConfigArgs}
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.{Bases, DistributedUtil, ReadSet, SparkCommand}
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

object VariantSupport {

  protected class Arguments extends DistributedUtil.Arguments with ReadLoadingConfigArgs {
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

    case class AlleleCount(sample: String,
                           contig: String,
                           locus: Long,
                           reference: String,
                           alternate: String,
                           count: Int) {
      override def toString: String = {
        s"$sample, $contig, $locus, $reference, $alternate, $count"
      }
    }

    override def run(args: Arguments, sc: SparkContext): Unit = {

      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)

      val adamContext = new ADAMContext(sc)

      val variants: RDD[Variant] = adamContext.loadVariants(args.variants)
      val reads: Seq[RDD[MappedRead]] = args.bams.zipWithIndex.map(
        bamFile =>
          ReadSet(
            sc,
            bamFile._1,
            ReadInputFilters.empty,
            config = ReadLoadingConfigArgs(args)
          ).mappedReads
      )

      // Build a loci set from the variant positions
      val lociSet = LociSet.union(
        variants.map(
          variant => LociSet(variant.getContig.getContigName, variant.getStart, variant.getEnd))
          .collect(): _*)

      val lociPartitions = DistributedUtil.partitionLociUniformly(args.parallelism, lociSet)

      val alleleCounts =
        reads.map(sampleReads =>
          DistributedUtil.pileupFlatMap[AlleleCount](
            sampleReads,
            lociPartitions,
            skipEmpty = true,
            pileupToAlleleCounts,
            reference = reference
          )
        ).reduce(_ ++ _)

      alleleCounts.saveAsTextFile(args.output)

    }

    /**
     * Count alleles in a pileup
     *
     * @param pileup Pileup of reads a given locu
     * @return Iterator of AlleleCount which contains pair of reference and alternate with a count
     */
    def pileupToAlleleCounts(pileup: Pileup): Iterator[AlleleCount] = {
      val alleles = pileup.elements.groupBy(_.allele)
      alleles.map(kv => AlleleCount(pileup.sampleName,
        pileup.referenceName,
        pileup.locus,
        Bases.basesToString(kv._1.refBases),
        Bases.basesToString(kv._1.altBases),
        kv._2.size)).iterator
    }
  }
}
