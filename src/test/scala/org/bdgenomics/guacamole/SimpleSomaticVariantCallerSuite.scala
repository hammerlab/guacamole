
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

package org.bdgenomics.guacamole

import org.bdgenomics.adam.avro.{ ADAMGenotype }
import org.scalatest.matchers.ShouldMatchers
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.OrderedRDDFunctions
import org.bdgenomics.guacamole.somatic.{ Reference, SimpleRead, SimpleSomaticVariantCaller }
import net.sf.samtools.SAMRecord
import org.bdgenomics.adam.projections.ADAMNucleotideContigFragmentField

class SimpleSomaticVariantCallerSuite extends TestUtil.SparkFunSuite with ShouldMatchers {

  def zipWithReferenceIndices(reference: String, contigName: String, start: Long = 0) = {
    val name = Reference.normalizeContigName(contigName)
    reference.zipWithIndex.map({ case (c, i) => ((name, start + i.toLong), c.toByte) }).toList
  }

  val sameStartReferenceSeq = "A" * 70
  val sameStartContigName = "artificial"
  val sameStartReferenceBases: List[((String, Long), Byte)] =
    zipWithReferenceIndices(sameStartReferenceSeq, sameStartContigName)
  val sameStartGlobalPositionBases: List[(Long, Byte)] = sameStartReferenceBases.map(pair => (pair._1._2, pair._2))
  val sameStartContigStart = Map[String, Long]("artificial" -> 70)

  val sameStartBasesAtGlobalPosition = sameStartReferenceBases.map(pair => (pair._1._2, pair._2))
  val sameStartIndex = Reference.Index(sameStartContigStart)

  def loadReads(filename: String, referenceIndex: Reference.Index): RDD[SimpleRead] = {
    /* grab the path to the SAM file we've stashed in the resources subdirectory */
    val path = ClassLoader.getSystemClassLoader.getResource(filename).getFile
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    val records = SimpleRead.loadFile(path, sc, referenceIndex)
    records
  }

  sparkTest("No repeated positions in pileup RDD") {
    val normalReads = loadReads("same_start_reads.sam", sameStartIndex)
    val normalPileups: RDD[(Long, SimpleSomaticVariantCaller.Pileup)] =
      SimpleSomaticVariantCaller.buildPileups(normalReads, sc.broadcast(sameStartIndex.contigStartArray))
    var seenPositions = Set[Long]()
    for ((pos, _) <- normalPileups.collect) {
      assert(!seenPositions.contains(pos), "Multiple RDD entries for position " + pos.toString)
      seenPositions += pos
    }
  }

  sparkTest("No variants when tumor/normal identical") {
    val reads = loadReads("same_start_reads.sam", sameStartIndex)
    val reference = Reference(sc.parallelize(sameStartBasesAtGlobalPosition), sameStartIndex)
    val genotypes = SimpleSomaticVariantCaller.callVariants(reads, reads, reference, minPartitions = 1)
    genotypes.collect.toList should have length (0)
  }

  sparkTest("Simple SNV in same_start_reads") {
    val normal = loadReads("same_start_reads.sam", sameStartIndex)
    val tumor = loadReads("same_start_reads_snv_tumor.sam", sameStartIndex)
    val reference = Reference(sc.parallelize(sameStartGlobalPositionBases), sameStartIndex)
    val genotypes: RDD[ADAMGenotype] =
      SimpleSomaticVariantCaller.callVariants(tumor, normal, reference, minPartitions = 1)
    genotypes.collect.toList should have length 1
  }

  // the SAM files start at (base 0) position 16, so fill in spots 0-16 of the reference with A's
  val noMdtagReference =
    "CCCTATTAACCACTCACGGGAGCTCTCCATGCATTTGGTATTTTCGTCTGGGGGGTCTGCACGCGATAGCATTGCGAGACGCTGGAGCCGGAGCACCCTA"
  val noMdtagReferenceBases = zipWithReferenceIndices(noMdtagReference, "chrM", 16)
  val noMdTagBasesAtGlobalPosition = noMdtagReferenceBases.map { case ((_, pos), base) => (pos, base) }
  val noMdTagIndex = Reference.Index(Map[String, Long]("M" -> noMdtagReference.length))

  sparkTest("Simple SNV from SAM files with soft clipped reads and no MD tags") {
    // tumor SAM file was modified by replacing C>G at positions 17-19
    val normal = loadReads("normal_without_mdtag.sam", noMdTagIndex)
    val tumor = loadReads("tumor_without_mdtag.sam", noMdTagIndex)
    val reference = Reference(sc.parallelize(noMdTagBasesAtGlobalPosition), noMdTagIndex)
    val genotypes: RDD[ADAMGenotype] =
      SimpleSomaticVariantCaller.callVariants(tumor, normal, reference, minPartitions = 1)
    val localGenotypes: List[ADAMGenotype] = genotypes.collect.toList
    println("# of variants: %d".format(localGenotypes.length))
    localGenotypes should have length 3
    for (offset: Int <- 0 to 2) {
      val genotype: ADAMGenotype = localGenotypes(offset)
      val variant = genotype.getVariant
      val pos = variant.getPosition
      pos should be(16 + offset)
      val altSeq = variant.getVariantAllele
      altSeq should have length 1
      val alt = altSeq.charAt(0)
      alt should be('G')
    }
  }

  sparkTest("Reference with multiple chromosomes") {
    // tumor SAM file was modified by replacing C>G at positions 17-19
    val normal = loadReads("normal_without_mdtag.sam", noMdTagIndex)
    val tumor = loadReads("tumor_without_mdtag.sam", noMdTagIndex)

    val chrM = zipWithReferenceIndices(noMdtagReference, "chrM", 16)
    val chrX = zipWithReferenceIndices(noMdtagReference, "chrX", 0)
    val chrY = zipWithReferenceIndices(noMdtagReference, "chrY", 100)
    val chr1 = zipWithReferenceIndices(noMdtagReference, "chr1", 100)
    val bases = chr1 ++ chrM ++ chrX ++ chrY
    val n = noMdtagReference.length
    val index = Reference.Index(Map[String, Long]("M" -> n, "X" -> n, "Y" -> n, "1" -> n))
    val basesAtGlobalPosition = bases.map { case (locus, base) => (index.locusToGlobalPosition(locus), base) }
    val reference = Reference(sc.parallelize(basesAtGlobalPosition), index)
    val genotypes: RDD[ADAMGenotype] =
      SimpleSomaticVariantCaller.callVariants(tumor, normal, reference, minPartitions = 1)
    val localGenotypes: List[ADAMGenotype] = genotypes.collect.toList
    println("# of variants: %d".format(localGenotypes.length))
    localGenotypes should have length 3
    for (offset: Int <- 0 to 2) {
      val genotype: ADAMGenotype = localGenotypes(offset)
      val variant = genotype.getVariant
      val pos = variant.getPosition
      pos should be(16 + offset)
      val altSeq = variant.getVariantAllele
      altSeq should have length 1
      val alt = altSeq.charAt(0)
      alt should be('G')
    }
  }

  /**
   * Scan through a FASTA reference file and return a particular contig sequence
   *
   * @param referencePath
   */
  def getReferenceSequence(referencePath: String, contig: String): String = {
    val lineIterator = scala.io.Source.fromFile(referencePath).getLines

    // read all the sequence lines until the description
    def accumulateResult(): String = {
      val result = new StringBuilder(1000000)
      for (line <- lineIterator) {
        if (line.startsWith(">")) { return result.toString }
        else { result.append(line) }
      }
      return result.toString
    }

    for (line <- lineIterator) {
      if (line.startsWith(">")) {
        val header = line.substring(1)
        val currContig = header.split(" ")(0)
        if (contig == currContig) { return accumulateResult() }
      }
    }
    sys.error("Couldn't find contig %s in reference %s".format(contig, referencePath))
  }

  sparkTest("Loading FASTA RDD should give same sequence as local") {
    val filename = "human_g1k_v37_chr1_59kb.fasta"
    val path = ClassLoader.getSystemClassLoader.getResource(filename).getFile
    val chr1Local = getReferenceSequence(path, "1")

    chr1Local should not be null

    assert(chr1Local.length > 59000, "Expected 59Kb of chromosome 1, got only %d".format(chr1Local.length))

    val reference = Reference.load(path, sc)
    val referenceGlobalPositions = reference.basesAtGlobalPositions.map(_._1).collect
    val minPosition = referenceGlobalPositions.min

    minPosition should be(0L)

    val maxPosition = referenceGlobalPositions.max

    println("Reference global positions min = %s, max = %s".format(minPosition, maxPosition))

    val sortedBases: RDD[(Reference.Locus, Byte)] =
      reference.basesAtGlobalPositions.map({
        case (pos, byte) =>
          val locus = reference.index.globalPositionToLocus(pos)
          (locus, byte)
      }).sortByKey(ascending = true)
    val collectedSortedBases = sortedBases.collect
    val onlyLoci = collectedSortedBases.map(_._1)
    val minLocus = onlyLoci.min

    minLocus should be(("1", 0L))

    val maxLocus = onlyLoci.max
    println("Reference loci  min = %s, max = %s".format(minLocus, maxLocus))

    val noLoci = collectedSortedBases.map(_._2)
    val baseArrayNoLoci = noLoci.map(_.toChar)

    assert(baseArrayNoLoci.length == chr1Local.length,
      "Distributed chromosome length %d != local length %d".format(
        baseArrayNoLoci.length,
        chr1Local.length))

    val distributedChromosomeString = baseArrayNoLoci.mkString("")
    distributedChromosomeString should be(chr1Local)
  }

}
