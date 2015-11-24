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

package org.hammerlab.guacamole.windowing

import org.hammerlab.guacamole.LociSet
import org.hammerlab.guacamole.util.TestUtil
import org.scalatest.{ FunSuite, Matchers }

class SlidingWindowSuite extends FunSuite with Matchers {

  test("test sliding read window, duplicate reads") {

    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1))
    val window = SlidingWindow("chr1", 2, reads.iterator)
    window.setCurrentLocus(0)
    assert(window.currentRegions.size === 3)

  }

  test("test sliding read window, diff contigs") {

    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1, "chr1"),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1, "chr2"),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1, "chr3"))
    val window = SlidingWindow("chr1", 2, reads.iterator)
    val caught = the[IllegalArgumentException] thrownBy { window.setCurrentLocus(0) }
    caught.getMessage should include("must have the same reference name")

  }

  test("test sliding read window, offset reads") {

    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 4),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 8))
    val window = SlidingWindow("chr1", 2, reads.iterator)

    window.setCurrentLocus(0)
    assert(window.currentRegions.size === 1)

    window.setCurrentLocus(4)
    assert(window.currentRegions.size === 2)

  }

  test("test sliding read window, reads are not sorted") {

    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 8),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 4))
    val window = SlidingWindow("chr1", 8, reads.iterator)
    val caught = the[IllegalArgumentException] thrownBy { window.setCurrentLocus(0) }
    caught.getMessage should include("Regions must be sorted by start locus")

  }

  test("test sliding read window, slow walk with halfWindowSize=0") {
    // 01234567890 position
    // .TCGATCGA.. #1
    // ..CGATCGAT. #2
    // .....TCG... #3
    // 01222333210 count
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("CGATCGAT", "8M", "8", 2),
      TestUtil.makeRead("TCG", "3M", "3", 5))
    val window = SlidingWindow("chr1", 0, reads.iterator)

    window.setCurrentLocus(0)
    assert(window.currentRegions.size === 0)

    window.setCurrentLocus(1)
    assert(window.currentRegions.size === 1)

    window.setCurrentLocus(2)
    assert(window.currentRegions.size === 2)

    window.setCurrentLocus(3)
    assert(window.currentRegions.size === 2)

    window.setCurrentLocus(4)
    assert(window.currentRegions.size === 2)

    window.setCurrentLocus(5)
    assert(window.currentRegions.size === 3)

    window.setCurrentLocus(6)
    assert(window.currentRegions.size === 3)

    window.setCurrentLocus(7)
    assert(window.currentRegions.size === 3)

    window.setCurrentLocus(8)
    assert(window.currentRegions.size === 2)

    window.setCurrentLocus(9)
    assert(window.currentRegions.size === 1)

    window.setCurrentLocus(10)
    assert(window.currentRegions.size === 0)
  }

  test("test sliding read window, slow walk with halfWindowSize=1") {
    // 0123456789012 position
    // ..TCGATCGA... #1
    // ...CGATCGAT.. #2
    // ......TCG.... #3
    // 0122233333210 count w/ hfS=1
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 2),
      TestUtil.makeRead("CGATCGAT", "8M", "8", 3),
      TestUtil.makeRead("TCG", "3M", "3", 6))
    val window = SlidingWindow("chr1", 1, reads.iterator)

    window.setCurrentLocus(0)
    assert(window.currentRegions.size === 0)

    window.setCurrentLocus(1)
    assert(window.currentRegions.size === 1)

    window.setCurrentLocus(2)
    assert(window.currentRegions.size === 2)

    window.setCurrentLocus(3)
    assert(window.currentRegions.size === 2)

    window.setCurrentLocus(4)
    assert(window.currentRegions.size === 2)

    window.setCurrentLocus(5)
    assert(window.currentRegions.size === 3)

    window.setCurrentLocus(6)
    assert(window.currentRegions.size === 3)

    window.setCurrentLocus(7)
    assert(window.currentRegions.size === 3)

    window.setCurrentLocus(8)
    assert(window.currentRegions.size === 3)

    window.setCurrentLocus(9)
    assert(window.currentRegions.size === 3)

    window.setCurrentLocus(10)
    assert(window.currentRegions.size === 2)

    window.setCurrentLocus(11)
    assert(window.currentRegions.size === 1)

    window.setCurrentLocus(12)
    assert(window.currentRegions.size === 0)
  }

  test("test sliding window advance multiple windows trivial 1") {
    val reads1 = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 2),
      TestUtil.makeRead("CGATCGAT", "8M", "8", 3),
      TestUtil.makeRead("TCG", "3M", "3", 6))
    val window1 = SlidingWindow("chr1", 0, reads1.iterator)

    val reads2 = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 2),
      TestUtil.makeRead("CGATCGAT", "8M", "8", 3),
      TestUtil.makeRead("TCG", "3M", "3", 6))
    val window2 = SlidingWindow("chr1", 0, reads2.iterator)

    val loci = LociSet.parse("chr1:0-3,chr1:20-30").result.onContig("chr1").iterator
    val windows = Seq(window1, window2)

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = false) should be(Some(0))
    windows.map(_.currentLocus) should equal(Seq(0, 0))
    windows.map(_.nextLocusWithRegions) should equal(Seq(Some(2), Some(2)))

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(2))
    windows.map(_.currentLocus) should equal(Seq(2, 2))

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(None)
  }

  test("test sliding window advance multiple windows trivial 2") {
    val reads1 = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("CGATCGAT", "8M", "8", 3),
      TestUtil.makeRead("TCG", "3M", "3", 6))
    val window1 = SlidingWindow("chr1", 1, reads1.iterator)

    val reads2 = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 2),
      TestUtil.makeRead("CGATCGAT", "8M", "8", 3),
      TestUtil.makeRead("TCG", "3M", "3", 6))
    val window2 = SlidingWindow("chr1", 0, reads2.iterator)

    val loci = LociSet.parse("chr1:0-3,chr1:20-30").result.onContig("chr1").iterator
    val windows = Seq(window1, window2)

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(0))
    windows.map(_.currentLocus) should equal(Seq(0, 0))
    windows.map(_.nextLocusWithRegions) should equal(Seq(Some(1), Some(2)))

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(1))
    windows.map(_.currentLocus) should equal(Seq(1, 1))

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(2))
    windows.map(_.currentLocus) should equal(Seq(2, 2))

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(None)
  }

  test("test sliding window advance multiple windows basic") {
    val reads1 = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 2),
      TestUtil.makeRead("CGATCGAT", "8M", "8", 3),
      TestUtil.makeRead("TCG", "3M", "3", 6))
    val window1 = SlidingWindow("chr1", 0, reads1.iterator)

    val reads2 = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 5),
      TestUtil.makeRead("CGATCGAT", "8M", "8", 80),
      TestUtil.makeRead("TCG", "3M", "3", 100))
    val window2 = SlidingWindow("chr1", 0, reads2.iterator)

    val loci = LociSet.parse("chr1:0-3,chr1:60-101").result.onContig("chr1").iterator
    val windows = Seq(window1, window2)

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = false) should be(Some(0))
    windows.map(_.currentLocus) should equal(Seq(0, 0))
    windows.map(_.nextLocusWithRegions) should equal(Seq(Some(2), Some(5)))

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(2))
    windows.map(_.currentLocus) should equal(Seq(2, 2))
    windows(0).currentRegions.isEmpty should be(false)
    windows(1).currentRegions.isEmpty should be(true)

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(80))
    windows.map(_.currentLocus) should equal(Seq(80, 80))
    windows(0).currentRegions.isEmpty should be(true)
    windows(1).currentRegions.isEmpty should be(false)

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(81))
    windows.map(_.currentLocus) should equal(Seq(81, 81))
    windows(0).currentRegions.isEmpty should be(true)
    windows(1).currentRegions.isEmpty should be(false)

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(82))
    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(83))
    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(84))
    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(85))
    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(86))
    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(87))
    windows.map(_.currentLocus) should equal(Seq(87, 87))
    windows(0).currentRegions.isEmpty should be(true)
    windows(1).currentRegions.isEmpty should be(false)

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(Some(100))
    windows.map(_.currentLocus) should equal(Seq(100, 100))
    windows(0).currentRegions.isEmpty should be(true)
    windows(1).currentRegions.isEmpty should be(false)

    SlidingWindow.advanceMultipleWindows(windows, loci, skipEmpty = true) should be(None)
  }
}
