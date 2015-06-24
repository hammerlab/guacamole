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

import org.hammerlab.guacamole.reads.Read
import org.hammerlab.guacamole.util.{ TestUtil, GuacFunSuite }
import org.scalatest.{ Matchers, FunSuite }
import org.bdgenomics.formats.avro.GenotypeAllele
import scala.collection.JavaConversions._
import org.hammerlab.guacamole.pileup.Pileup

class GermlineThresholdCallerSuite extends GuacFunSuite with Matchers {

  test("no variants, threshold 0") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 0)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Ref, GenotypeAllele.Ref)))
  }

  test("het variant, threshold 0") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 0)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Ref, GenotypeAllele.Alt)))

  }

  test("het variant, threshold 30") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 30)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Ref, GenotypeAllele.Alt)))

  }

  test("het variant, threshold 50, not enough evidence") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 50)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Ref, GenotypeAllele.Ref)))
  }

  test("homozygous alt variant, threshold 50") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1),
      TestUtil.makeRead("GCGATCGA", "8M", "0T7", 1))
    val pileup = Pileup(reads, 1)
    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 50, emitRef = false)
    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Alt, GenotypeAllele.Alt)))

    genotypes.length should be(1)
    genotypes.head.getVariant.getStart should be(1)
    genotypes.head.getVariant.getReferenceAllele.toString should be("T")
    genotypes.head.getVariant.getAlternateAllele.toString should be("G")
  }

  test("homozygous alt variant, threshold 50; no reference bases observed") {
    val reads = Seq(
      TestUtil.makeRead("TGGATCGA", "8M", "1C6", 1),
      TestUtil.makeRead("TGGATCGA", "8M", "1C6", 1),
      TestUtil.makeRead("TGGATCGA", "8M", "1C6", 1))
    val pileup = Pileup(reads, 2)
    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 50, emitRef = false)

    genotypes.length should be(1)
    genotypes.head.getVariant.getStart should be(2)
    genotypes.head.getVariant.getReferenceAllele.toString should be("C")
    genotypes.head.getVariant.getAlternateAllele.toString should be("G")

    genotypes.foreach(gt => assert(gt.getAlleles.toList === List(GenotypeAllele.Alt, GenotypeAllele.Alt)))
  }

  // Regression test for https://github.com/hammerlab/guacamole/issues/302
  sparkTest("heterozygous deletion") {
    val filters = Read.InputFilters(mapped = true, nonDuplicate = true, passedVendorQualityChecks = true)
    val reads = TestUtil.loadReads(sc, "synthetic.challenge.set1.normal.v2.withMDTags.chr2.syn1fp.sam", filters=filters).mappedReads.collect()
    val pileup = Pileup(reads, 16050070)

    val genotypes = GermlineThreshold.Caller.callVariantsAtLocus(pileup, 8  /* 8% variant allele fraction */, emitRef = false)
    genotypes.length should be(1)
    val variant = genotypes.head.getVariant
    variant.getStart should be(16050070)
    variant.getReferenceAllele.toString should be("T")
    variant.getAlternateAllele.toString should be("")
  }
}
