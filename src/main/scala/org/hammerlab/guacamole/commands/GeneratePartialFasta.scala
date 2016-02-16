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

import java.io.{ BufferedWriter, FileWriter, File }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.Variant
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reads.Read.InputFilters
import org.hammerlab.guacamole.reference.{ ContigNotFound, ReferenceBroadcast }
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

/**
 * This command is used to generate a "partial fasta" which we use in our tests of variant callers. It should be run
 * locally. It is not intended for any large-scale or production use.
 *
 * A "partial fasta" is a fasta file where the reference names look like "chr1:9242255-9242454/249250621". That gives
 * the contig name, the start and end locus, and the total contig size. The associated sequence in the file gives the
 * reference sequence for just the sites between start and end.
 *
 * This lets us package up a subset of a reference fasta into a file that is small enough to version control and
 * distribute.
 *
 */
object GeneratePartialFasta {

  protected class Arguments extends DistributedUtil.Arguments with Common.Arguments.ReadLoadingConfigArgs with Common.Arguments.Reference {
    @Args4jOption(name = "--output", metaVar = "OUT", required = true, aliases = Array("-o"),
      usage = "Output path for partial fasta")
    var output: String = ""

    @Args4jOption(name = "--reference-fasta", required = true, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = null

    @Argument(required = true, multiValued = true,
      usage = "Reads to write out overlapping fasta sequence for")
    var bams: Array[String] = Array.empty
  }

  object Caller extends SparkCommand[Arguments] {

    override val name = "generate-partial-fasta"
    override val description = """
        |Given a full fasta and some loci (either specified directly or from reads), write out a fasta containing only
        |the subset of the reference overlapped by the loci in a guacamole-specific fasta format
      """.trim.stripMargin

    override def run(args: Arguments, sc: SparkContext): Unit = {

      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)
      val lociBuilder = Common.lociFromArguments(args, default = "none")
      val readSets = args.bams.zipWithIndex.map(fileAndIndex =>
        ReadSet(
          sc,
          fileAndIndex._1,
          requireMDTagsOnMappedReads = false,
          InputFilters.empty,
          token = fileAndIndex._2,
          contigLengthsFromDictionary = true,
          referenceGenome = None,
          config = Common.Arguments.ReadLoadingConfigArgs.fromArguments(args)))

      val reads = sc.union(readSets.map(_.mappedReads))
      val contigLengths = readSets.head.contigLengths

      val regions = reads.map(read => (read.referenceContig, read.start, read.end))
      regions.collect.foreach(triple => {
        lociBuilder.put(triple._1, triple._2, Some(triple._3))
      })

      val loci = lociBuilder.result

      val fd = new File(args.output)
      val writer = new BufferedWriter(new FileWriter(fd))
      loci.contigs.foreach(contig => {
        loci.onContig(contig).ranges.foreach(range => {
          try {
            val sequence = Bases.basesToString(reference.getContig(contig).slice(range.start.toInt, range.end.toInt))
            writer.write(">%s:%d-%d/%d\n".format(contig, range.start, range.end, contigLengths(contig)))
            writer.write(sequence)
            writer.write("\n")
          } catch {
            case e: ContigNotFound => log.warn("No such contig in reference: %s: %s".format(contig, e.toString))
          }
        })
      })
      writer.close()
      Common.progress("Wrote: %s".format(args.output))
    }
  }
}
