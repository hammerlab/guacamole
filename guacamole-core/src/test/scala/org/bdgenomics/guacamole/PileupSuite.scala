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

import org.bdgenomics.adam.avro.ADAMRecord
import org.scalatest.matchers.ShouldMatchers
import org.bdgenomics.adam.rich.DecadentRead
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD

class PileupSuite extends TestUtil.SparkFunSuite with ShouldMatchers {

  def loadADAMRecords(filename: String): RDD[ADAMRecord] = {
    /* grab the path to the SAM file we've stashed in the resources subdirectory */
    val path = ClassLoader.getSystemClassLoader.getResource(filename).getFile
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    sc.adamLoad(path)
  }

  def loadPileup(filename: String, locus: Long = 0): Pileup = {
    val records = loadADAMRecords(filename)
    val localReads = records.collect.map(DecadentRead(_))
    Pileup(localReads, locus)
  }

  sparkTest("Load pileup from SAM file") {
    val pileup = loadPileup("same_start_reads.sam", 0)
    pileup.elements.length should be(10)
  }

  sparkTest("First 60 loci should have all 10 reads") {
    val pileup = loadPileup("same_start_reads.sam", 0)
    for (i <- 1 to 59) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.length should be(10)
    }
  }

  sparkTest("Loci 10-19 deleted from half of the reads") {
    val pileup = loadPileup("same_start_reads.sam", 0)
    for (i <- 10 to 19) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.filter(_.isDeletion).length should be(5)
    }
  }

  sparkTest("Loci 60-69 have 5 reads") {
    val pileup = loadPileup("same_start_reads.sam", 0)
    for (i <- 60 to 69) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.length should be(5)
    }
  }


  sparkTest("Pileup.Element basic test") {
    intercept[NullPointerException] {
      val e = Pileup.Element(null, 42)
    }
    val adamRecords = loadADAMRecords("different_start_reads.sam").collect()

    val read1Record = adamRecords(0)
    val decadentRead1 = new DecadentRead(read1Record)

    // read1 starts at SAM:6 → 0-based 5
    // and has CIGAR: 29M10D31M
    // so, the length is 70
    intercept[AssertionError] { Pileup.Element(decadentRead1, 0) }
    intercept[AssertionError] { Pileup.Element(decadentRead1, 4) }
    intercept[AssertionError] { Pileup.Element(decadentRead1, 5 + 70) }
    val at5 = Pileup.Element(decadentRead1, 5)
    assert(at5 != null)
    assert(at5.sequenceRead == "A")
    assert(at5.singleBaseRead == 'A')

    // At the end of the read:
    assert(Pileup.Element(decadentRead1, 74) != null)
    intercept[AssertionError] { Pileup.Element(decadentRead1, 75) }

    // Just before the deletion
    val at28 = Pileup.Element(decadentRead1, 5 + 28)
    assert(at28.sequenceRead === "A")
    // Inside the deletion
    val at29 = Pileup.Element(decadentRead1, 5 + 29)
    assert(at29.sequenceRead === "")
    val at38 = Pileup.Element(decadentRead1, 5 + 38)
    assert(at38.sequenceRead === "")
    // Just after the deletion
    val at39 = Pileup.Element(decadentRead1, 5 + 39)
    assert(at39.sequenceRead === "A")

    //  `read2` has an insertion: 5M5I34M10D16M
    val read2Record = adamRecords(1) // read2
    val decadentRead2 = new DecadentRead(read2Record)
    val read2At10 = Pileup.Element(decadentRead2, 10)
    assert(read2At10 != null)
    assert(read2At10.sequenceRead === "A")
    // inside the insertion
    val read2At15 = Pileup.Element(decadentRead2, 15)
    assert(read2At15.sequenceRead === "AAAAA")
    val read2At16 = Pileup.Element(decadentRead2, 16)
    assert(read2At16.sequenceRead === "AAAA")
    val read2At19 = Pileup.Element(decadentRead2, 19)
    assert(read2At19.sequenceRead === "A")
    // right after the insert
    val read2At20 = Pileup.Element(decadentRead2, 20)
    assert(read2At20.sequenceRead === "A")

    // elementAtGreaterLocus is a no-op on the same locus, 
    // and fails in lower loci
    val loci = Seq(5, 33, 34, 43, 44, 74)
    loci.map({ l =>
      val elt = Pileup.Element(decadentRead1, l)
      assert(elt.elementAtGreaterLocus(l) === elt)
      intercept[AssertionError] { elt.elementAtGreaterLocus(l - 1) }
      intercept[AssertionError] { elt.elementAtGreaterLocus(75) }
    })

    val read3Record = adamRecords(2) // read3
    val decadentRead3 = new DecadentRead(read3Record)
    val read3At15 = Pileup.Element(decadentRead3, 15)
    assert(read3At15 != null)
    assert(read3At15.sequenceRead == "A")
    assert(read3At15.elementAtGreaterLocus(16).sequenceRead == "T")
    assert(read3At15.elementAtGreaterLocus(17).sequenceRead == "C")
    assert(read3At15.elementAtGreaterLocus(16).elementAtGreaterLocus(17).sequenceRead == "C")
    assert(read3At15.elementAtGreaterLocus(18).sequenceRead == "G")

    // Read4 has CIGAR: 10M10I10D40M
    // It's ACGT repeated 15 times
    val decadentRead4 = new DecadentRead(adamRecords(3))
    val read4At20 = Pileup.Element(decadentRead4, 20)
    assert(read4At20 != null)
    for (i <- 0 to 14) {
      assert(read4At20.elementAtGreaterLocus(20 + i * 4 + 0).sequenceRead(0) == 'A')
      assert(read4At20.elementAtGreaterLocus(20 + i * 4 + 1).sequenceRead(0) == 'C')
      assert(read4At20.elementAtGreaterLocus(20 + i * 4 + 2).sequenceRead(0) == 'G')
      assert(read4At20.elementAtGreaterLocus(20 + i * 4 + 3).sequenceRead(0) == 'T')
    }

    // Read5: ACGTACGTACGTACG, 5M4=1X5=, [10; 25[
    //        MMMMM====G=====
    if (false) { // This test fails because of issue #30
      val decadentRead5 = new DecadentRead(adamRecords(4))
      val read5At10 = Pileup.Element(decadentRead5, 10)
      assert(read5At10 != null)
      assert(read5At10.elementAtGreaterLocus(10).sequenceRead === "A")
      assert(read5At10.elementAtGreaterLocus(14).sequenceRead === "A")
      assert(read5At10.elementAtGreaterLocus(18).sequenceRead === "A")
      assert(read5At10.elementAtGreaterLocus(19).sequenceRead === "C")
      assert(read5At10.elementAtGreaterLocus(20).sequenceRead === "G")
      assert(read5At10.elementAtGreaterLocus(21).sequenceRead === "T")
      assert(read5At10.elementAtGreaterLocus(22).sequenceRead === "A")
      assert(read5At10.elementAtGreaterLocus(24).sequenceRead === "G")
    }
  }
}

