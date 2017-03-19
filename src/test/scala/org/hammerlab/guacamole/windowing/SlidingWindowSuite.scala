package org.hammerlab.guacamole.windowing

import org.hammerlab.genomics.loci.set.test.LociSetUtil
import org.hammerlab.genomics.reads.ReadsUtil
import org.hammerlab.genomics.reference.test.LociConversions._
import org.hammerlab.guacamole.windowing.SlidingWindow.advanceMultipleWindows
import org.hammerlab.test.Suite

class SlidingWindowSuite
  extends Suite
    with ReadsUtil
    with LociSetUtil {

  test("test sliding read window, duplicate reads") {

    val reads =
      makeReads(
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1)
      )

    val window = SlidingWindow("chr1", 2, reads.iterator)
    window.setCurrentLocus(0)
    assert(window.currentRegions.size == 3)
  }

  test("test sliding read window, diff contigs") {

    val reads =
      Seq(
        makeRead("TCGATCGA", "8M", 1, "chr1"),
        makeRead("TCGATCGA", "8M", 1, "chr2"),
        makeRead("TCGATCGA", "8M", 1, "chr3")
      )

    val window = SlidingWindow("chr1", 2, reads.iterator)
    val caught = the[IllegalArgumentException] thrownBy { window.setCurrentLocus(0) }
    caught.getMessage should include("must have the same reference name")
  }

  test("test sliding read window, offset reads") {

    val reads =
      makeReads(
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 4),
        ("TCGATCGA", "8M", 8)
      )

    val window = SlidingWindow("chr1", 2, reads.iterator)

    window.setCurrentLocus(0)
    assert(window.currentRegions.size == 1)

    window.setCurrentLocus(4)
    assert(window.currentRegions.size == 2)
  }

  test("test sliding read window, reads are not sorted") {

    val reads =
      makeReads(
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 8),
        ("TCGATCGA", "8M", 4)
      )

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
    val reads =
      makeReads(
        ("TCGATCGA", "8M", 1),
        ("CGATCGAT", "8M", 2),
        ("TCG", "3M", 5)
      )

    val window = SlidingWindow("chr1", 0, reads.iterator)

    window.setCurrentLocus(0)
    assert(window.currentRegions.size == 0)

    window.setCurrentLocus(1)
    assert(window.currentRegions.size == 1)

    window.setCurrentLocus(2)
    assert(window.currentRegions.size == 2)

    window.setCurrentLocus(3)
    assert(window.currentRegions.size == 2)

    window.setCurrentLocus(4)
    assert(window.currentRegions.size == 2)

    window.setCurrentLocus(5)
    assert(window.currentRegions.size == 3)

    window.setCurrentLocus(6)
    assert(window.currentRegions.size == 3)

    window.setCurrentLocus(7)
    assert(window.currentRegions.size == 3)

    window.setCurrentLocus(8)
    assert(window.currentRegions.size == 2)

    window.setCurrentLocus(9)
    assert(window.currentRegions.size == 1)

    window.setCurrentLocus(10)
    assert(window.currentRegions.size == 0)
  }

  test("test sliding read window, slow walk with halfWindowSize=1") {
    // 0123456789012 position
    // ..TCGATCGA... #1
    // ...CGATCGAT.. #2
    // ......TCG.... #3
    // 0122233333210 count w/ hfS=1
    val reads =
      makeReads(
        ("TCGATCGA", "8M", 2),
        ("CGATCGAT", "8M", 3),
        ("TCG", "3M", 6)
      )

    val window = SlidingWindow("chr1", 1, reads.iterator)

    window.setCurrentLocus(0)
    assert(window.currentRegions.size == 0)

    window.setCurrentLocus(1)
    assert(window.currentRegions.size == 1)

    window.setCurrentLocus(2)
    assert(window.currentRegions.size == 2)

    window.setCurrentLocus(3)
    assert(window.currentRegions.size == 2)

    window.setCurrentLocus(4)
    assert(window.currentRegions.size == 2)

    window.setCurrentLocus(5)
    assert(window.currentRegions.size == 3)

    window.setCurrentLocus(6)
    assert(window.currentRegions.size == 3)

    window.setCurrentLocus(7)
    assert(window.currentRegions.size == 3)

    window.setCurrentLocus(8)
    assert(window.currentRegions.size == 3)

    window.setCurrentLocus(9)
    assert(window.currentRegions.size == 3)

    window.setCurrentLocus(10)
    assert(window.currentRegions.size == 2)

    window.setCurrentLocus(11)
    assert(window.currentRegions.size == 1)

    window.setCurrentLocus(12)
    assert(window.currentRegions.size == 0)
  }

  test("test sliding window advance multiple windows trivial 1") {
    val reads1 =
      makeReads(
        ("TCGATCGA", "8M", 2),
        ("CGATCGAT", "8M", 3),
        ("TCG", "3M", 6)
      )

    val window1 = SlidingWindow("chr1", 0, reads1.iterator)

    val reads2 =
      makeReads(
        ("TCGATCGA", "8M", 2),
        ("CGATCGAT", "8M", 3),
        ("TCG", "3M", 6)
      )

    val window2 = SlidingWindow("chr1", 0, reads2.iterator)

    val loci = lociSet("chr1:0-3,chr1:20-30").apply("chr1").iterator
    val windows = Vector(window1, window2)

    advanceMultipleWindows(windows, loci, skipEmpty = false) should ===(Some(0))
    windows.map(_.currentLocus) should ===(Seq(0, 0))
    windows.map(_.nextLocusWithRegions) should ===(Seq(Some(2), Some(2)))

    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(2))
    windows.map(_.currentLocus) should ===(Seq(2, 2))

    advanceMultipleWindows(windows, loci, skipEmpty = true) should be(None)
  }

  test("test sliding window advance multiple windows trivial 2") {
    val reads1 =
      makeReads(
        ("TCGATCGA", "8M", 0),
        ("CGATCGAT", "8M", 3),
        ("TCG", "3M", 6)
      )

    val window1 = SlidingWindow("chr1", 1, reads1.iterator)

    val reads2 =
      makeReads(
        ("TCGATCGA", "8M", 2),
        ("CGATCGAT", "8M", 3),
        ("TCG", "3M", 6)
      )

    val window2 = SlidingWindow("chr1", 0, reads2.iterator)

    val loci = lociSet("chr1:0-3,chr1:20-30").apply("chr1").iterator
    val windows = Vector(window1, window2)

    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(0))
    windows.map(_.currentLocus) should ===(Seq(0, 0))
    windows.map(_.nextLocusWithRegions) should ===(Seq(Some(1), Some(2)))

    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(1))
    windows.map(_.currentLocus) should ===(Seq(1, 1))

    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(2))
    windows.map(_.currentLocus) should ===(Seq(2, 2))

    advanceMultipleWindows(windows, loci, skipEmpty = true) should be(None)
  }

  test("test sliding window advance multiple windows basic") {
    val reads1 =
      makeReads(
        ("TCGATCGA", "8M", 2),
        ("CGATCGAT", "8M", 3),
        ("TCG", "3M", 6)
      )

    val window1 = SlidingWindow("chr1", 0, reads1.iterator)

    val reads2 =
      makeReads(
        ("TCGATCGA", "8M", 5),
        ("CGATCGAT", "8M", 80),
        ("TCG", "3M", 100)
      )

    val window2 = SlidingWindow("chr1", 0, reads2.iterator)

    val loci = lociSet("chr1:0-3,chr1:60-101").apply("chr1").iterator
    val windows = Vector(window1, window2)

    advanceMultipleWindows(windows, loci, skipEmpty = false) should ===(Some(0))
    windows.map(_.currentLocus) should ===(Seq(0, 0))
    windows.map(_.nextLocusWithRegions) should ===(Seq(Some(2), Some(5)))

    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(2))
    windows.map(_.currentLocus) should ===(Seq(2, 2))
    windows(0).currentRegions.isEmpty should be(false)
    windows(1).currentRegions.isEmpty should be(true)

    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(80))
    windows.map(_.currentLocus) should ===(Seq(80, 80))
    windows(0).currentRegions.isEmpty should be(true)
    windows(1).currentRegions.isEmpty should be(false)

    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(81))
    windows.map(_.currentLocus) should ===(Seq(81, 81))
    windows(0).currentRegions.isEmpty should be(true)
    windows(1).currentRegions.isEmpty should be(false)

    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(82))
    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(83))
    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(84))
    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(85))
    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(86))
    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(87))
    windows.map(_.currentLocus) should ===(Seq(87, 87))
    windows(0).currentRegions.isEmpty should be(true)
    windows(1).currentRegions.isEmpty should be(false)

    advanceMultipleWindows(windows, loci, skipEmpty = true) should ===(Some(100))
    windows.map(_.currentLocus) should ===(Seq(100, 100))
    windows(0).currentRegions.isEmpty should be(true)
    windows(1).currentRegions.isEmpty should be(false)

    advanceMultipleWindows(windows, loci, skipEmpty = true) should be(None)
  }
}
