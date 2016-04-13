package org.hammerlab.guacamole.loci.map

import com.google.common.collect.{ImmutableRangeMap, Range}
import org.hammerlab.guacamole.util.GuacFunSuite

class ContigSuite extends GuacFunSuite {
  test("empty") {
    type JLong = java.lang.Long
    val contigMap = new Contig("chr1", ImmutableRangeMap.builder[JLong, String]().build())

    contigMap.get(100) should be(None)
    contigMap.getAll(0, 10000) should equal(Set())
    contigMap.contains(100) should be(false)
    contigMap.count should be(0)
    contigMap.ranges should equal(Set())
    contigMap.numRanges should be(0)
    contigMap.isEmpty should be(true)
    contigMap.toString should be("")
  }

  test("basic operations") {
    type JLong = java.lang.Long
    val range100to200 = ImmutableRangeMap.of[JLong, String](Range.closedOpen[JLong](100, 200), "A")
    val range200to300 = ImmutableRangeMap.of[JLong, String](Range.closedOpen[JLong](200, 300), "B")

    val contigMap = new Contig("chr1",
      ImmutableRangeMap.builder[JLong, String]().putAll(range100to200).putAll(range200to300).build())

    contigMap.get(99) should be(None)
    contigMap.get(100) should be(Some("A"))
    contigMap.get(199) should be(Some("A"))
    contigMap.get(200) should be(Some("B"))
    contigMap.get(299) should be(Some("B"))
    contigMap.get(300) should be(None)

    contigMap.getAll(0, 100) should equal(Set())
    contigMap.getAll(0, 101) should equal(Set("A"))
    contigMap.getAll(199, 200) should equal(Set("A"))
    contigMap.getAll(199, 201) should equal(Set("A", "B"))
    contigMap.getAll(200, 201) should equal(Set("B"))
    contigMap.getAll(0, 10000) should equal(Set("A", "B"))

    contigMap.contains(0) should be(false)
    contigMap.contains(99) should be(false)
    contigMap.contains(100) should be(true)
    contigMap.contains(200) should be(true)
    contigMap.contains(299) should be(true)
    contigMap.contains(300) should be(false)

    contigMap.count should be(200)
    contigMap.isEmpty should be(false)
    contigMap.toString should be("chr1:100-200=A,chr1:200-300=B")

    val emptyMap = new Contig("chr1", ImmutableRangeMap.builder[JLong, String]().build())
    contigMap.union(contigMap) should equal(contigMap)
    contigMap.union(emptyMap) should equal(contigMap)

    val emptyMapWrongContig = new Contig("chr2", ImmutableRangeMap.builder[JLong, String]().build())
    val caught = the[AssertionError] thrownBy { contigMap.union(emptyMapWrongContig) }
    caught.getMessage should include("different contigs: chr1 and chr2")
  }

  test("getAll") {
    val lociMap = LociMap.newBuilder[Long]().put("chrM", 0, 8286, 0L).put("chrM", 8286, 16571, 1L).result
    lociMap.onContig("chrM").getAll(5, 10) should equal(Set[Long](0L))
    lociMap.onContig("chrM").getAll(10000, 11000) should equal(Set[Long](1L))
  }
}
