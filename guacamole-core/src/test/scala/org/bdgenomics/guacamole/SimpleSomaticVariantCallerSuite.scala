
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

import org.bdgenomics.adam.avro.{ ADAMGenotype, ADAMRecord }
import org.scalatest.matchers.ShouldMatchers
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.callers.SimpleSomaticVariantCaller

class SimpleSomaticVariantCallerSuite extends TestUtil.SparkFunSuite with ShouldMatchers {

  def loadReads(filename: String): RDD[ADAMRecord] = {
    /* grab the path to the SAM file we've stashed in the resources subdirectory */
    val path = ClassLoader.getSystemClassLoader.getResource(filename).getFile
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    sc.adamLoad(path)
  }

  sparkTest("No repeated positions in pileup RDD") {
    val normalReads = loadReads("same_start_reads.sam")
    val normalPileups: RDD[(Long, SimpleSomaticVariantCaller.Pileup)] =
      SimpleSomaticVariantCaller.buildPileups(normalReads)
    var seenPositions = Set[Long]()
    for ((pos, _) <- normalPileups.collect) {
      assert(!seenPositions.contains(pos), "Multiple RDD entries for position " + pos.toString)
      seenPositions += pos
    }
  }

  sparkTest("All BaseReads in pileup should have same position") {
    val normalReads = loadReads("same_start_reads.sam")
    val normalPileups: RDD[(Long, SimpleSomaticVariantCaller.Pileup)] =
      SimpleSomaticVariantCaller.buildPileups(normalReads)
    for ((pos, pileup) <- normalPileups.collect) {
      for (baseRead: SimpleSomaticVariantCaller.BaseRead <- pileup) {
        baseRead.locus should be(pos)
      }
    }
  }

  sparkTest("No variants when tumor/normal identical") {
    val reads = loadReads("same_start_reads.sam")
    val genotypes = SimpleSomaticVariantCaller.callVariants(reads, reads)
    genotypes.collect.toList should have length (0)
  }

  sparkTest("Simple SNV in same_start_reads") {
    val normal: RDD[ADAMRecord] = loadReads("same_start_reads.sam")
    val tumor: RDD[ADAMRecord] = loadReads("same_start_reads_snv_tumor.sam")
    val genotypes: RDD[ADAMGenotype] = SimpleSomaticVariantCaller.callVariants(normal, tumor)
    genotypes.collect.toList should have length 1
  }

  sparkTest("Simple SNV from SAM files with soft clipped reads and no MD tags") {
    // tumor SAM file was modified by replacing C>G at positions 17-19
    val normal: RDD[ADAMRecord] = loadReads("normal_without_mdtag.sam")
    val tumor: RDD[ADAMRecord] = loadReads("tumor_without_mdtag.sam")
    val genotypes: RDD[ADAMGenotype] = SimpleSomaticVariantCaller.callVariants(normal, tumor)
    val localGenotypes: List[ADAMGenotype] = genotypes.collect.toList
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

}

