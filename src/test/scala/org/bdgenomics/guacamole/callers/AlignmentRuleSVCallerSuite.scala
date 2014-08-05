package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.TestUtil
import org.scalatest.matchers.ShouldMatchers
import org.bdgenomics.guacamole.TestUtil.SparkFunSuite

class AlignmentRuleSVCallerSuite extends SparkFunSuite with ShouldMatchers {

  test("no variants; reads properly aligned") {
    val reads = Seq(
      TestUtil.makePairedRead(chr = "chr1",
        start = 10L,
        isPositiveStrand = true,
        isMateMapped = true,
        mateReferenceContig = Some("chr1"),
        mateStart = Some(100L),
        isMatePositiveStrand = false),
      TestUtil.makePairedRead(chr = "chr1",
        start = 12L,
        isPositiveStrand = true,
        isMateMapped = true,
        mateReferenceContig = Some("chr1"),
        mateStart = Some(100L),
        isMatePositiveStrand = false))

    val breakpoint = AlignmentRuleSVCaller.discoverBreakpoint(reads)
    breakpoint.isDefined should be(false)
  }

  test("intrachromosomal variant") {
    val reads = Seq(
      TestUtil.makePairedRead(chr = "chr1",
        start = 10L,
        isPositiveStrand = true,
        isMateMapped = true,
        mateReferenceContig = Some("chr6"),
        mateStart = Some(100L),
        isMatePositiveStrand = false),
      TestUtil.makePairedRead(chr = "chr1",
        start = 12L,
        isPositiveStrand = true,
        isMateMapped = true,
        mateReferenceContig = Some("chr1"),
        mateStart = Some(100L),
        isMatePositiveStrand = false))

    val breakpoint = AlignmentRuleSVCaller.discoverBreakpoint(reads)
    breakpoint.isDefined should be(true)
  }

  test("unmapped mate") {
    val reads = Seq(
      TestUtil.makePairedRead(chr = "chr1",
        start = 10L,
        isPositiveStrand = true,
        isMateMapped = false),
      TestUtil.makePairedRead(chr = "chr1",
        start = 12L,
        isPositiveStrand = true,
        isMateMapped = true,
        mateReferenceContig = Some("chr1"),
        mateStart = Some(100L),
        isMatePositiveStrand = false))

    val breakpoint = AlignmentRuleSVCaller.discoverBreakpoint(reads)
    breakpoint.isDefined should be(true)
  }

  test("inversion variant") {
    val reads = Seq(
      TestUtil.makePairedRead(chr = "chr1",
        start = 10L,
        isPositiveStrand = true,
        isMateMapped = true,
        mateReferenceContig = Some("chr1"),
        mateStart = Some(100L),
        isMatePositiveStrand = true),
      TestUtil.makePairedRead(chr = "chr1",
        start = 12L,
        isPositiveStrand = true,
        isMateMapped = true,
        mateReferenceContig = Some("chr1"),
        mateStart = Some(100L),
        isMatePositiveStrand = false))

    val breakpoint = AlignmentRuleSVCaller.discoverBreakpoint(reads)
    breakpoint.isDefined should be(true)
  }

  test("duplication variant") {
    val reads = Seq(
      TestUtil.makePairedRead(chr = "chr1",
        start = 10L,
        isPositiveStrand = false,
        isMateMapped = true,
        mateReferenceContig = Some("chr1"),
        mateStart = Some(100L),
        isMatePositiveStrand = true),
      TestUtil.makePairedRead(chr = "chr1",
        start = 12L,
        isPositiveStrand = true,
        isMateMapped = true,
        mateReferenceContig = Some("chr1"),
        mateStart = Some(100L),
        isMatePositiveStrand = false))

    val breakpoint = AlignmentRuleSVCaller.discoverBreakpoint(reads)
    breakpoint.isDefined should be(true)
  }

  test("duplication variant where mate is first read") {
    val reads = Seq(
      TestUtil.makePairedRead(chr = "chr1",
        start = 100L,
        isPositiveStrand = true,
        isMateMapped = true,
        mateReferenceContig = Some("chr1"),
        mateStart = Some(300L),
        isMatePositiveStrand = false),
      TestUtil.makePairedRead(chr = "chr1",
        start = 100L,
        isPositiveStrand = true,
        isMateMapped = true,
        mateReferenceContig = Some("chr1"),
        mateStart = Some(20L),
        isMatePositiveStrand = false,
        isFirstInPair = false))

    val breakpoint = AlignmentRuleSVCaller.discoverBreakpoint(reads)
    breakpoint.isDefined should be(true)
  }

  sparkTest("testing inversion structural variants") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc,
      "tumor.chr2.inversion.sam",
      "normal.chr2.inversion.sam")

    val breakpoint = AlignmentRuleSVCaller.discoverBreakpoint(tumorReads)
    breakpoint.isDefined should be(true)

  }

  sparkTest("testing duplication structural variants") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc,
      "tumor.chr2.duplication.sam",
      "normal.chr2.duplication.sam")

    val breakpoint = AlignmentRuleSVCaller.discoverBreakpoint(tumorReads)
    breakpoint.isDefined should be(true)

  }

}
