package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.reads.TestRegion
import org.hammerlab.guacamole.util.GuacFunSuite

class ContigIteratorSuite
  extends GuacFunSuite
    with RegionsUtil {

  test("simple") {
    ContigIterator(
      makeRegions(
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
        makeRegions(
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
