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

package org.bdgenomics.guacamole.callers

import org.bdgenomics.adam.avro._
import org.bdgenomics.guacamole.{ Common, Command }
import org.bdgenomics.guacamole.Common.Arguments.{ TumorNormalReads, Output, Base }
import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.{ mutable, JavaConversions }
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.{ RichADAMRecord }
import net.sf.samtools.{ CigarOperator }

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
    with Common.Arguments.TumorNormalReads {}

  def log[T](msg: String, rdd: RDD[T]) = {
    val array = rdd.collect()
    val list = array.toList
    println(msg + "(len =" + list.length.toString + ") : " + list.toString.substring(0, 100))
  }

  /**
   * Alignment that's almost identical to Pileup.Element.Alignment except that
   * Insertion has one character rather than a full string. The expected usage is
   * that loci should also include all positions *in between* two reference
   * positions, and that there will be a cleanup pass at the end of variant
   * calling which combines multiple insertions into one genotype.
   */
  abstract class Alignment
  case class Match(base: Char) extends Alignment
  case class Mismatch(base: Char) extends Alignment
  case class Insertion(base: Char) extends Alignment
  case class Deletion() extends Alignment

  /**
   * Instead of building explicit pileups, we're splitting apart the bases into their own
   * data structure and grouping them by the positions they occur at. Thus, Seq[BaseRead] acts
   * implicitly like a pileup at a locus.
   *
   * Type parameter T corresponds to locus type.
   */
  case class BaseRead(
    val locus: Long,
    val alignment: Alignment,
    val readQuality: Option[Int], // don't have read quality for deletions
    val alignmentQuality: Int)

  type Pileup = Seq[BaseRead]

  /**
   *  Create a simple BaseRead object from the CIGAR operator and other information about a read at a
   *  given position `readPos`.
   *
   *  @param cigarOp
   *  @param record
   *  @param readPos
   *  @param alignmentQuality
   *
   */
  def makeBaseReads(cigarOp: CigarOperator, record: RichADAMRecord, readPos: Int, alignmentQuality: Int) = {

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
        val base: Char = record.sequence(readPos)
        val readQuality = record.qualityScores(readPos)
        Some(BaseRead(
          alignment = Match(base),
          locus = readPos,
          readQuality = Some(readQuality),
          alignmentQuality = alignmentQuality))
      // mismatch
      case CigarOperator.X =>
        val base: Char = record.sequence(readPos)
        val readQuality = record.qualityScores(readPos)
        Some(BaseRead(
          alignment = Mismatch(base),
          locus = readPos,
          readQuality = Some(readQuality),
          alignmentQuality = alignmentQuality))
      case CigarOperator.MATCH_OR_MISMATCH =>
        val base: Char = record.sequence(readPos)
        val readQuality = record.qualityScores(readPos)
        val alignment = if (record.mdTag.get.isMatch(readPos)) Match(base) else Mismatch(base)
        Some(BaseRead(
          alignment = alignment,
          locus = readPos,
          readQuality = Some(readQuality),
          alignmentQuality = alignmentQuality))
      case CigarOperator.DELETION =>
        Some(BaseRead(
          alignment = Deletion(),
          readQuality = None,
          locus = readPos,
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
  def expandBaseReads(record: ADAMRecord): Pileup = {
    val richRecord = RichADAMRecord(record)
    val alignmentQuality = record.mapq
    val baseReads = mutable.MutableList[BaseRead]()
    var refPos = richRecord.start.toInt
    var readPos = refPos - richRecord.unclippedStart.get.toInt
    for (cigarElt <- richRecord.samtoolsCigar.getCigarElements) {
      val cigarOp = cigarElt.getOperator
      val nBases = cigarElt.getLength
      baseReads ++= makeBaseReads(cigarOp, record, readPos, alignmentQuality)
      if (cigarOp.consumesReferenceBases) { refPos += nBases }
      if (cigarOp.consumesReadBases) { readPos += nBases }
    }
    baseReads
  }

  /**
   * Expand a collection of ADAMRecords into an RDD that's keyed by chrosomosomal positions,
   * whose values are a collection of BaseReads at that position.
   *
   * @param reads Aligned reads
   * @return RDD of position, pileup pairs
   */
  def buildPileups(reads: RDD[ADAMRecord]): RDD[(Long, Seq[BaseRead])] = {
    val baseReadsAtPos: RDD[BaseRead] = reads.flatMap(expandBaseReads _)
    baseReadsAtPos.groupBy(_.locus)
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
  def buildGenotype(ref: Char, alt: Char, locus: Long): ADAMGenotype = {
    val contig: ADAMContig = ADAMContig.newBuilder.setContigName("reference").build
    val variant: ADAMVariant =
      ADAMVariant.newBuilder
        .setPosition(locus)
        .setReferenceAllele(ref.toString)
        .setVariantAllele(alt.toString)
        .setContig(contig)
        .build

    val alleles = List(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt)

    ADAMGenotype.newBuilder()
      .setAlleles(List(ADAMGenotypeAllele.Alt))
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
  def baseCounts(pileup: Pileup): Map[Alignment, Long] = {
    var map = Map[Alignment, Long]()
    for (baseRead <- pileup) {
      val key: Alignment = baseRead.alignment
      val oldCount: Long = map.get(key) getOrElse 0
      val newCount: Long = oldCount + 1
      map += (key -> newCount)
    }
    map
  }

  /**
   * Call a single somatic variant from the normal and tumor reads at a particular locus.
   *
   * @param locus Location on the chromosome.
   * @param normalPileup Collection of bases read from normal sample which align to this locus.
   * @param tumorPileup Collection of bases read from tumor sample which to this locus.
   * @return An optional ADAMGenotype denoting the variant called, or None if there is no variant.
   */
  def callVariantGenotype(locus: Long, normalPileup: Pileup, tumorPileup: Pileup): Option[ADAMGenotype] = {
    val tumorBaseCounts = baseCounts(tumorPileup)
    val normalBaseCounts = baseCounts(normalPileup)
    val (tumorMaxAlignment, tumorMaxCount) = tumorBaseCounts.maxBy(_._2)
    val (normalMaxAlignment, normalMaxCount) = normalBaseCounts.maxBy(_._2)
    (normalMaxAlignment, tumorMaxAlignment) match {
      case (Match(a), Mismatch(b)) if a != b =>
        Some(buildGenotype(a, b, locus))
      case other =>
        None
    }
  }

  /**
   *
   * @param normalReads Unsorted collection of ADAMRecords representing short reads from normal tissue.
   * @param tumorReads Short reads from tumor tissue.
   * @return An RDD of genotypes representing the somatic variants discovered.
   */
  def callVariants(normalReads: RDD[ADAMRecord], tumorReads: RDD[ADAMRecord]): RDD[ADAMGenotype] = {
    val normalPileups: RDD[(Long, Pileup)] = buildPileups(normalReads)
    val tumorPileups: RDD[(Long, Pileup)] = buildPileups(tumorReads)
    val joinedPileups: RDD[(Long, (Pileup, Pileup))] = normalPileups.join(tumorPileups)
    val sortedPileups = joinedPileups.sortByKey()
    sortedPileups.flatMap({
      case (pos, (normalPileup, tumorPileup)) => callVariantGenotype(pos, normalPileup, tumorPileup)
    })
  }

  /**
   * Load the tumor and normal reads from commandline arguments and run the variant caller.
   *
   * @param context An ADAM spark context which is used to load read data into RDDs.
   * @param args An arguments object containing the filepaths to tumor and normal read files.
   * @return Somatic variants in an RDD of genotypes.
   */
  def callVariantsFromArgs(context: ADAMContext, args: TumorNormalReads): RDD[ADAMGenotype] = {
    val (tumorReads, normalReads) =
      Common.loadTumorNormalReads(args, context, mapped = true, nonDuplicate = true)
    callVariants(normalReads, tumorReads)
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val context: ADAMContext = Common.createSparkContext(args)
    val genotypes: RDD[ADAMGenotype] = callVariantsFromArgs(context, args)
    Common.writeVariants(args, genotypes)
  }
}