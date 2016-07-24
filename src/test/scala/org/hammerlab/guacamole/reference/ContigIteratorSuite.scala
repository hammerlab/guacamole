package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.reads.{ReadsUtil, TestRegion}
import org.scalatest.{FunSuite, Matchers}

class ContigIteratorSuite extends FunSuite with Matchers with ReadsUtil {
  test("simple") {
    ContigIterator(
      makeReads(
        List(
          ("chr1", 100, 200, 2),
          ("chr1", 110, 210, 1),
          ("chr2", 100, 200, 3)
        )
      )
    ).toList should be(
      List(
        TestRegion("chr1", 100, 200),
        TestRegion("chr1", 100, 200),
        TestRegion("chr1", 110, 210)
      )
    )
  }

  test("next past end") {
    val it =
      ContigIterator(
        makeReads(
          List(
            ("chr1", 100, 200, 1),
            ("chr1", 110, 210, 1),
            ("chr2", 100, 200, 3)
          )
        )
      )

    it.hasNext should be(true)
    it.next() should be(TestRegion("chr1", 100, 200))
    it.hasNext should be(true)
    it.next() should be(TestRegion("chr1", 110, 210))
    it.hasNext should be(false)
    intercept[NoSuchElementException] {
      it.next()
    }
  }
}
