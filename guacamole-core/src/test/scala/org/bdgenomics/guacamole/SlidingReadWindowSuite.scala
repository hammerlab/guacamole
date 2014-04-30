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

import org.scalatest.FunSuite
import org.bdgenomics.adam.avro.{ ADAMRecord, ADAMContig }
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.matchers._

class SlidingReadWindowSuite extends FunSuite {

  def makeRead(sequence: String,
               cigar: String,
               mdtag: String,
               start: Long = 1,
               chr: String = "chr1"): ADAMRecord = {

    val contig = ADAMContig.newBuilder()
      .setContigName(chr)
      .build()

    ADAMRecord.newBuilder()
      .setReadName("read")
      .setStart(start)
      .setReadMapped(true)
      .setCigar(cigar)
      .setSequence(sequence)
      .setMapq(60)
      .setQual(sequence.map(x => 'F').toString)
      .setMismatchingPositions(mdtag)
      .setRecordGroupSample("sample")
      .setContig(contig)
      .build()
  }

  test("test sliding read window, duplicate reads") {

    val reads = Seq(makeRead("TCGATCGA", "8M", "8", 1),
      makeRead("TCGATCGA", "8M", "8", 1),
      makeRead("TCGATCGA", "8M", "8", 1))
    val window = SlidingReadWindow(2, reads.iterator)
    window.setCurrentLocus(0)
    assert(window.currentReads.size === 3)

  }

  test("test sliding read window, diff contigs") {

    val reads = Seq(makeRead("TCGATCGA", "8M", "8", 1, "chr1"),
      makeRead("TCGATCGA", "8M", "8", 1, "chr2"),
      makeRead("TCGATCGA", "8M", "8", 1, "chr3"))
    val window = SlidingReadWindow(2, reads.iterator)
    val caught = evaluating { window.setCurrentLocus(0) } should produce[IllegalArgumentException]
    caught.getMessage should include("must have the same reference name")

  }

  test("test sliding read window, offset reads") {

    val reads = Seq(makeRead("TCGATCGA", "8M", "8", 1),
      makeRead("TCGATCGA", "8M", "8", 4),
      makeRead("TCGATCGA", "8M", "8", 8))
    val window = SlidingReadWindow(2, reads.iterator)

    window.setCurrentLocus(0)
    assert(window.currentReads.size === 1)

    window.setCurrentLocus(4)
    assert(window.currentReads.size === 2)

  }

  test("test sliding read window, reads are not sorted") {

    val reads = Seq(makeRead("TCGATCGA", "8M", "8", 1),
      makeRead("TCGATCGA", "8M", "8", 8),
      makeRead("TCGATCGA", "8M", "8", 4))
    val window = SlidingReadWindow(8, reads.iterator)
    val caught = evaluating { window.setCurrentLocus(0) } should produce[IllegalArgumentException]
    caught.getMessage should include("Reads must be sorted by start locus")

  }

  test("test sliding read window, halfWindowSize=0") {
    // 01234567890 position
    // .TCGATCGA.. #1
    // ..CGATCGAT. #2
    // .....TCG... #3
    // 01222333210 count
    val reads = Seq(makeRead("TCGATCGA", "8M", "8", 1),
      makeRead("CGATCGAT", "8M", "8", 2),
      makeRead("TCG", "3M", "3", 5))
    val window = SlidingReadWindow(0, reads.iterator)

    window.setCurrentLocus(0)
    assert(window.currentReads.size === 0)

    window.setCurrentLocus(1)
    assert(window.currentReads.size === 1)

    window.setCurrentLocus(2)
    assert(window.currentReads.size === 2)

    window.setCurrentLocus(3)
    assert(window.currentReads.size === 2)

    window.setCurrentLocus(4)
    assert(window.currentReads.size === 2)

    window.setCurrentLocus(5)
    assert(window.currentReads.size === 3)

    window.setCurrentLocus(6)
    assert(window.currentReads.size === 3)

    window.setCurrentLocus(7)
    assert(window.currentReads.size === 3)

    window.setCurrentLocus(8)
    assert(window.currentReads.size === 2)

    window.setCurrentLocus(9)
    assert(window.currentReads.size === 1)

    window.setCurrentLocus(10)
    assert(window.currentReads.size === 0)
  }

  // Tests to write:
  // - Slow walk through with halfWindowSize=1

}
