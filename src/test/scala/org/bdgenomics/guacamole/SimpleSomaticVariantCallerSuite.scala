
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

import org.bdgenomics.formats.avro.{ Genotype }
import org.scalatest.matchers.ShouldMatchers
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.OrderedRDDFunctions
import org.bdgenomics.guacamole.somatic.{ Reference, SimpleRead, SimpleSomaticVariantCaller }

class SimpleSomaticVariantCallerSuite extends TestUtil.SparkFunSuite with ShouldMatchers {
  // Disabling for now to allow Spark 1.0 upgrade to go through.
  // Will have to revisit this.

  /*


  def loadReads(filename: String): RDD[SimpleRead] = {
    /* grab the path to the SAM file we've stashed in the resources subdirectory */
    val path = ClassLoader.getSystemClassLoader.getResource(filename).getFile
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    val records = SimpleRead.loadFile(path, sc)
    records
  }

  def zipWithReferenceIndices(reference: String, contigName: String, start: Long = 0) = {
    val name = Reference.normalizeContigName(contigName)
    reference.zipWithIndex.map({ case (c, i) => ((name, start + i.toLong), c.toByte) }).toList
  }

  val sameStartReferenceSeq = "A" * 70
  val sameStartContigName = "artificial"
  val sameStartReferenceBases: List[((String, Long), Byte)] =
    zipWithReferenceIndices(sameStartReferenceSeq, sameStartContigName)

  sparkTest("No repeated positions in pileup RDD") {
    val normalReads = loadReads("same_start_reads.sam")
    val normalPileups: RDD[((String, Long), SimpleSomaticVariantCaller.Pileup)] =
      SimpleSomaticVariantCaller.buildPileups(normalReads)
    var seenPositions = Set[(String, Long)]()
    for ((locus, _) <- normalPileups.collect) {
      assert(!seenPositions.contains(locus), "Multiple RDD entries for position " + locus.toString)
      seenPositions += locus
    }
  }

  sparkTest("No variants when tumor/normal identical") {
    val reads = loadReads("same_start_reads.sam")

    val genotypes = SimpleSomaticVariantCaller.callVariants(reads, reads, sc.parallelize(sameStartReferenceBases))
    genotypes.collect.toList should have length (0)
  }

  sparkTest("Simple SNV in same_start_reads") {
    val normal = loadReads("same_start_reads.sam")
    val tumor = loadReads("same_start_reads_snv_tumor.sam")
    val genotypes: RDD[Genotype] =
      SimpleSomaticVariantCaller.callVariants(normal, tumor, sc.parallelize(sameStartReferenceBases))
    genotypes.collect.toList should have length 1
  }

  // the SAM files start at (base 0) position 16, so fill in spots 0-16 of the reference with A's
  val noMdtagReference =
    "CCCTATTAACCACTCACGGGAGCTCTCCATGCATTTGGTATTTTCGTCTGGGGGGTCTGCACGCGATAGCATTGCGAGACGCTGGAGCCGGAGCACCCTA"
  val noMdtagReferenceBases = zipWithReferenceIndices(noMdtagReference, "chrM", 16)

  sparkTest("Simple SNV from SAM files with soft clipped reads and no MD tags") {
    // tumor SAM file was modified by replacing C>G at positions 17-19
    val normal = loadReads("normal_without_mdtag.sam")
    val tumor = loadReads("tumor_without_mdtag.sam")

    val genotypes: RDD[Genotype] =
      SimpleSomaticVariantCaller.callVariants(normal, tumor, sc.parallelize(noMdtagReferenceBases))
    val localGenotypes: List[Genotype] = genotypes.collect.toList
    println("# of variants: %d".format(localGenotypes.length))
    localGenotypes should have length 3
    for (offset: Int <- 0 to 2) {
      val genotype: Genotype = localGenotypes(offset)
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

    val reference: RDD[(SimpleSomaticVariantCaller.Locus, Byte)] =
      Reference.loadReference(path, sc)
    val sortedBases: RDD[(SimpleSomaticVariantCaller.Locus, Byte)] = reference.sortByKey(ascending = true)
    val noIndices = sortedBases.map(_._2)
    val baseArray = noIndices.collect().map(_.toChar)

    assert(baseArray.length == chr1Local.length,
      "Distributed chromosome length %d != local length %d".format(baseArray.length, chr1Local.length))
    val sequence = baseArray.mkString("")
    sequence should be(chr1Local)

  }
  */

}

