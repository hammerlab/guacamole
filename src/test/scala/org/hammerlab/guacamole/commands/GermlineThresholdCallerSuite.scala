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

package org.hammerlab.guacamole.commands

import org.bdgenomics.formats.avro.GenotypeAllele
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.ReadInputFilters
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}

import scala.collection.JavaConversions._

class GermlineThresholdCallerSuite extends GuacFunSuite {

  def reference = TestUtil.makeReference(sc, Seq(
    ("chr1", 0, "ATCGATCGA"),
    ("2", 16050070, "T")))
  def referenceContigSequence = reference.getContig("chr1")

  sparkTest("no variants, threshold 0") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1))
    val pileup = Pileup(reads, "chr1", 1, referenceContigSequence = referenceContigSequence)
    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 0)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Ref, GenotypeAllele.Ref)))
  }

  sparkTest("het variant, threshold 0") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("GCGATCGA", "8M", 1))
    val pileup = Pileup(reads, "chr1", 1, referenceContigSequence = referenceContigSequence)
    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 0)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Ref, GenotypeAllele.Alt)))

  }

  sparkTest("het variant, threshold 30") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("GCGATCGA", "8M", 1))
    val pileup = Pileup(reads, "chr1", 1, referenceContigSequence = referenceContigSequence)
    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 30)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Ref, GenotypeAllele.Alt)))

  }

  sparkTest("het variant, threshold 50, not enough evidence") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("GCGATCGA", "8M", 1))
    val pileup = Pileup(reads, "chr1", 1, referenceContigSequence = referenceContigSequence)
    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 50)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Ref, GenotypeAllele.Ref)))
  }

  sparkTest("homozygous alt variant, threshold 50") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("GCGATCGA", "8M", 1),
      TestUtil.makeRead("GCGATCGA", "8M", 1))
    val pileup = Pileup(reads, "chr1", 1, referenceContigSequence = referenceContigSequence)
    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 50, emitRef = false)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Alt, GenotypeAllele.Alt)))

    genotypes.length should be(1)
    genotypes.head.getVariant.getStart should be(1)
    genotypes.head.getVariant.getReferenceAllele should be("T")
    genotypes.head.getVariant.getAlternateAllele should be("G")
  }

  sparkTest("homozygous alt variant, threshold 50; no reference bases observed") {
    val reads = Seq(
      TestUtil.makeRead("TGGATCGA", "8M", 1),
      TestUtil.makeRead("TGGATCGA", "8M", 1),
      TestUtil.makeRead("TGGATCGA", "8M", 1))
    val pileup = Pileup(reads, "chr1", 2, referenceContigSequence = referenceContigSequence)
    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 50, emitRef = false)

    genotypes.length should be(1)
    genotypes.head.getVariant.getStart should be(2)
    genotypes.head.getVariant.getAlternateAllele should be("G")
    genotypes.head.getVariant.getReferenceAllele should be("C")

    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Alt, GenotypeAllele.Alt)))
  }

  // Regression test for https://github.com/hammerlab/guacamole/issues/302
  sparkTest("heterozygous deletion") {
    val filters = ReadInputFilters(mapped = true, nonDuplicate = true, passedVendorQualityChecks = true)
    val reads =
      TestUtil.loadReads(
        sc,
        "synthetic.challenge.set1.normal.v2.withMDTags.chr2.syn1fp.sam",
        filters = filters
      ).mappedReads.collect()

    val pileup = Pileup(reads, "2", 16050070, referenceContigSequence = reference.getContig("2"))

    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 8 /* 8% variant allele fraction */ , emitRef = false)

    // Calling a variant here would be fine, but GermlineThresholdCaller isn't that smart.
    genotypes.length should be(0)
  }
}
