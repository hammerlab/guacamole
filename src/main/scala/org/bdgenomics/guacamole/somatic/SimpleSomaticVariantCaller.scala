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

package org.bdgenomics.guacamole.somatic

import org.bdgenomics.adam.avro._
import org.bdgenomics.guacamole.{ Common, Command }
import org.bdgenomics.guacamole.Common.Arguments.{ Output, Base }
import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.{ mutable, JavaConversions }
import org.bdgenomics.adam.rdd.ADAMContext._
import net.sf.samtools.{ CigarOperator }
import org.bdgenomics.guacamole.somatic.Reference.Locus
import scala.Some
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer
import org.kohsuke.args4j.{ Option => Opt }


/**
 * Simple somatic variant caller implementation.
 *
 * Call variants when:
 *  - there are more than 10 reads for both tumor and normal with mapping quality > 20
 *  - the most common reads for normal are reference matches
 *  - the most common reads for tumor are an alternate
 */

object SimpleSomaticVariantCaller extends Command {
  override val name = "somatic"
  override val description = "somatic (cancer) variant calling from tumor and normal reads, using simple thresholds"

  private class Arguments extends Base
    with Output
    with Common.Arguments.TumorNormalReads
    with Common.Arguments.Reference {

    @Opt(name = "-parallelism", usage = "Min number of partitions for reference and reads")
    var parallelism: Int = 1000
  }

  /**
   * Instead of building explicit pileups, we're splitting apart the bases into their own
   * data structure and grouping them by the positions they occur at. Thus, Seq[BaseRead] acts
   * implicitly like a pileup at a locus.
   *
   * Doesn't handle insertions but deletions can be expressed as base = None
   */
  case class BaseRead(base: Byte,
                      readQuality: Byte, // don't have read quality for deletions
                      alignmentQuality: Byte)

  type Pileup = Seq[BaseRead]

  /**
   * Expand a collection of ADAMRecords into an RDD that's keyed by chrosomosomal positions,
   * whose values are a collection of BaseReads at that position.
   *
   * @param reads Aligned reads
   * @param minBaseQuality Discard bases whose read quality falls below this threshold.
   * @param minDepth Discard pileups of size smaller than this parameter.
   * @param partitioner
   * @return RDD of position, pileup pairs
   */
  def buildPileups(reads: RDD[SimpleRead],
                   broadcastContigStart: Broadcast[Array[Long]],
                   minBaseQuality: Int = 0,
                   minDepth: Int = 0,
                   partitioner: Option[Partitioner] = None): RDD[(Long, Pileup)] = {
    var baseReadsAtPos: RDD[(Long, BaseRead)] = reads.mapPartitions({
      iter =>
        val result = ArrayBuffer[(Long, BaseRead)]()
        while (iter.hasNext) {
          val simpleRead = iter.next
          result ++=
            expandBaseReads(simpleRead, broadcastContigStart, minBaseQuality)
        }
        result.iterator
    })
    baseReadsAtPos = partitioner match {
      case Some(p) => baseReadsAtPos.partitionBy(p)
      case None    => baseReadsAtPos
    }
    var pileups = baseReadsAtPos.groupByKey()
    if (minDepth > 0) { pileups = pileups.filter(_._2.length >= minDepth) }
    pileups
  }

  /**
   *  Create a simple BaseRead object from the CIGAR operator and other information about a read at a
   *  given position `readPos`.
   *
   *  @param cigarOp
   *  @param refPos
   *  @param readPos
   *  @param readSequence
   *  @param qualityScores
   *  @param alignmentQuality
   */
  def makeBaseReads(cigarOp: CigarOperator,
                    refPos: Long,
                    readPos: Int,
                    readSequence: Array[Byte],
                    qualityScores: Array[Byte],
                    alignmentQuality: Byte): Option[BaseRead] = {

    /**
     * Table of cigar operators
     * Op BAM Description
     * M  0   alignment match (can be a sequence match or mismatch)
     * I  1   insertion to the reference
     * D  2   deletion from the reference
     * N  3   skipped region from the reference
     * S  4   soft clipping (clipped sequences present in SEQ)
     * H  5   hard clipping (clipped sequences NOT present in SEQ)
     * P  6   padding (silent deletion from padded reference)
     * =  7   sequence match
     * X  8   sequence mismatch
     */

    cigarOp match {
      // match
      case CigarOperator.EQ =>
        val base: Byte = readSequence(readPos)
        val readQuality = qualityScores(readPos)
        Some(BaseRead(
          base = base,
          readQuality = readQuality,
          alignmentQuality = alignmentQuality))
      // mismatch
      case CigarOperator.X =>
        val base: Byte = readSequence(readPos)
        val readQuality = qualityScores(readPos).toByte
        Some(BaseRead(
          base = base,
          readQuality = readQuality,
          alignmentQuality = alignmentQuality))
      case CigarOperator.MATCH_OR_MISMATCH =>
        val base: Byte = readSequence(readPos)
        val readQuality = qualityScores(readPos)
        Some(BaseRead(
          base = base,
          readQuality = readQuality,
          alignmentQuality = alignmentQuality))
      case CigarOperator.DELETION =>
        Some(BaseRead(
          base = 'D',
          readQuality = 0,
          alignmentQuality = alignmentQuality))
      // insertion (code I)
      case CigarOperator.INSERTION      => None
      // hard clip, reads absent from sequence, code = H
      case CigarOperator.HARD_CLIP      => None
      // soft clip, reads absent from sequence, code = S
      case CigarOperator.SOFT_CLIP      => None
      case CigarOperator.SKIPPED_REGION => None
      case CigarOperator.PADDING        => None
      case other                        => throw new RuntimeException("unexpected cigar operator: " + other.toString)
    }
  }

  /**
   * Given a single ADAM Record, expand it into all the base reads it contains
   *
   * @param record Single short-read (contains multiple bases)
   * @return Sequence of bases contained in input
   */
  def expandBaseReads(record: SimpleRead, referenceContigStart: Broadcast[Array[Long]], minBaseQuality: Int = 0): Seq[(Long, BaseRead)] = {
    val alignmentQuality = record.alignmentQuality
    assume(alignmentQuality >= 0, "Expected non-negative alignment quality, got %d".format(alignmentQuality))
    // using unclipped start since we're considering
    // soft clipping as one of the possible CIGAR operators
    // in makeBaseReads (which returns None for clipped positions)
    var refPos: Long = record.unclippedStart
    assume(refPos >= 0, "Expected non-negative unclipped start, got %d".format(refPos))
    var readPos = 0
    val result = mutable.ArrayBuffer[(Long, BaseRead)]()
    val baseSequence: Array[Byte] = record.baseSequence
    val qualityScores: Array[Byte] = record.baseQualities
    val contig = record.referenceContig
    val cigar: net.sf.samtools.Cigar = record.cigar
    for (cigarElt <- cigar.getCigarElements) {
      val cigarOp = cigarElt.getOperator
      // emit one BaseRead per position in the cigar element
      for (i <- 1 to cigarElt.getLength) {
        makeBaseReads(cigarOp, refPos, readPos, baseSequence, qualityScores, alignmentQuality) match {
          case None => ()
          case Some(baseRead) =>
            if (baseRead.readQuality >= minBaseQuality) {
              val start: Long = referenceContigStart.value(contig)
              val position = start + refPos
              result += ((position, baseRead))
            }
        }
        if (cigarOp.consumesReferenceBases) { refPos += 1 }
        if (cigarOp.consumesReadBases) { readPos += 1 }
      }
    }
    result.toSeq
  }

  /**
   * Helper function to build ADAM genotype objects with all the syntactic noise
   * of setSomeProperty methods called on a builder. This function isn't really done
   * yet, since it currently always emits Alt/Alt variants on the same contig.
   *
   * TODO: Generalize this to full range of variants we might encounter in
   * somatic variant calling and pass the chromosome as an argument.
   *
   * @param ref Reference allele at this locus
   * @param alt Alternate allele
   * @param locus Position on a chromosome
   * @return ADAM Genotype object corresponding to this variant.
   */
  def buildGenotype(ref: Byte, alt: Byte, locus: Locus): ADAMGenotype = {
    val (contigName, pos) = locus
    val contig: ADAMContig = ADAMContig.newBuilder.setContigName(contigName).build
    val variant: ADAMVariant =
      ADAMVariant.newBuilder
        .setPosition(pos)
        .setReferenceAllele(ref.toChar.toString)
        .setVariantAllele(alt.toChar.toString)
        .setContig(contig)
        .build

    val alleles = List(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt)

    ADAMGenotype.newBuilder()
      .setVariant(variant)
      .setAlleles(JavaConversions.seqAsJavaList(alleles))
      .setSampleId("sample".toCharArray)
      .build
  }

  /**
   *
   * Count how many times each kind of read alignment (i.e. Match(_)/Mismatch(x)/Deletion/Insertion(xyz))
   * occurs in the pileup.
   *
   * @param pileup Collection of bases read which aligned to a particular location.
   * @return Map from Alignment objects such as Match('T') to their count in the pileup.
   */
  def baseCountMap(pileup: Pileup): Map[Byte, Int] = {
    var map = Map[Byte, Int]()
    for (baseRead <- pileup) {
      val key: Byte = baseRead.base
      val oldCount: Int = map.getOrElse(key, 0)
      val newCount: Int = oldCount + 1
      map += (key -> newCount)
    }
    map
  }

  /**
   * Create a sorted list of Alignments paired with the number of times they occurred in a pileup.
   */
  def baseCounts(pileup: Pileup): List[(Byte, Int)] = {
    val map = baseCountMap(pileup)
    /* sorted list in decreasing order */
    map.toList.sortBy({ case (_, count) => -count })
  }

  /**
   *
   * Return a set of the most frequent bases in a pileup.
   *
   * @param pileup
   * @param percentileRank
   * @return
   */
  def topBases(pileup: Pileup, percentileRank: Int): mutable.Set[Byte] = {
    val total = pileup.length
    val sortedCounts: List[(Byte, Int)] = baseCounts(pileup)
    val result = mutable.Set[Byte]()
    var cumulative = 0
    for ((alignment, count) <- sortedCounts) {
      cumulative += count
      result += alignment
      if (((100 * cumulative) / total) >= percentileRank) {
        return result
      }
    }
    return result
  }

  /**
   *
   * @param normalReads Unsorted collection of ADAMRecords representing short reads from normal tissue.
   * @param tumorReads Short reads from tumor tissue.
   * @return An RDD of genotypes representing the somatic variants discovered.
   */
  def callVariants(tumorReads: RDD[SimpleRead],
                   normalReads: RDD[SimpleRead],
                   reference: Reference,
                   minBaseQuality: Int = 20,
                   minNormalCoverage: Int = 10,
                   minTumorCoverage: Int = 10,
                   minPartitions : Int = 500): RDD[ADAMGenotype] = {

    Common.progress("Entered callVariants")

    val sc = tumorReads.context
    val referenceIndex = reference.index

    val maxPartitions = (referenceIndex.numLoci / 10000L + 1).toInt
    val tumorPartitions = tumorReads.partitions.size
    val numPartitions: Int = Math.min(Math.max(minPartitions, tumorPartitions), maxPartitions)

    val broadcastContigStart = sc.broadcast(referenceIndex.contigStartArray)
    val tumorKeyed = tumorReads.keyBy(read => read.start + broadcastContigStart.value(read.referenceContig))
    val normalKeyed = normalReads.keyBy(read => read.start + broadcastContigStart.value(read.referenceContig))
    val partitioner: Partitioner = new RangePartitioner(numPartitions, tumorKeyed)

    val tumorPartitioned = tumorKeyed.partitionBy(partitioner).values
    val normalPartitioned = normalKeyed.partitionBy(partitioner).values

    val tumorPileups =
      buildPileups(tumorPartitioned, broadcastContigStart, minBaseQuality, minTumorCoverage, Some(partitioner))

    Common.progress("-- Tumor pileups: %s with %d partitions".format(
      tumorPileups.getClass, tumorPileups.partitions.size))

    val normalPileups =
      buildPileups(normalPartitioned, broadcastContigStart, minBaseQuality, minNormalCoverage, Some(partitioner))

    Common.progress("-- Normal pileups: %s with %d partitions".format(
      normalPileups.getClass, normalPileups.partitions.size))

    val referenceBases = reference.basesAtGlobalPositions.partitionBy(partitioner)
    Common.progress("partitioned reference")

    val tumorNormalRef: RDD[(Long, ((Pileup, Pileup), Byte))] = tumorPileups.join(normalPileups).join(referenceBases)

    Common.progress("Joined tumor+normal+reference genome: %s)".format(tumorNormalRef.toDebugString))

    val genotypes: RDD[(Long, (Byte, Byte))] = tumorNormalRef.mapPartitions({
      iter =>
        val result = ArrayBuffer[(Long, (Byte, Byte))]()
        while (iter.hasNext) {
          val (globalPosition, ((tumorPileup, normalPileup), ref)) = iter.next
          val normalBases: mutable.Set[Byte] = topBases(normalPileup, 90)
          val tumorBaseCounts = baseCounts(tumorPileup)
          val (alt, _) = tumorBaseCounts.maxBy(_._2)
          if (!normalBases.contains(alt) && alt != ref) {
            result += ((globalPosition, (ref, alt)))
          }
        }
        result.iterator
    }, preservesPartitioning = true)

    Common.progress("Raw genotype calls: %s)".format(genotypes.toDebugString))
    val sortedGenotypes = genotypes.sortByKey()

    val broadcastIndex = sc.broadcast(referenceIndex)
    val adamGenotypes: RDD[ADAMGenotype] = sortedGenotypes.map {
      case (position, (ref, alt)) =>
        val locus = broadcastIndex.value.globalPositionToLocus(position)
        buildGenotype(ref, alt, locus)
    }
    Common.progress("Done calling genotypes: %s".format(adamGenotypes.toDebugString))
    adamGenotypes
  }

  /**
   * Load the tumor and normal reads from commandline arguments and run the variant caller.
   *
   * @param sc An ADAM spark context which is used to load read data into RDDs.
   * @param args An arguments object containing the filepaths to tumor and normal read files.
   * @return Somatic variants in an RDD of genotypes.
   */

  def callVariantsFromArgs(sc: SparkContext, args: Arguments): RDD[ADAMGenotype] = {

    val referencePath = args.referenceInput
    val reference = Reference.load(referencePath, sc, numSplits = Some(args.parallelism))
    val normalReads: RDD[SimpleRead] = SimpleRead.loadFile(args.normalReads, sc, reference.index, true, true)
    val tumorReads: RDD[SimpleRead] = SimpleRead.loadFile(args.tumorReads, sc, reference.index, true, true)
    callVariants(tumorReads, normalReads, reference, minPartitions = args.parallelism)
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val context: SparkContext = Common.createSparkContext(args)
    val genotypes: RDD[ADAMGenotype] = callVariantsFromArgs(context, args)
    val path = args.variantOutput.stripMargin
    Common.progress("Found %d variants".format(genotypes.count))
    Common.writeVariantsFromArguments(args, genotypes)

  }
}