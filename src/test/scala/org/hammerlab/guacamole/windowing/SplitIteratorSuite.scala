package org.hammerlab.guacamole.windowing

import org.scalatest.{ FunSuite, Matchers }

class SplitIteratorSuite extends FunSuite with Matchers {
  val data1 = Seq(
    (0, "a"),
    (0, "b"),
    (3, "c"),
    (2, "d"),
    (3, "e"),
    (0, "f"),
    (1, "g"),
    (1, "h"),
    (3, "i"),
    (2, "j"),
    (3, "k"),
    (1, "l"),
    (0, "m"),
    (0, "n"),
    (0, "o")
  )

  test("test split iterator elements") {
    val iterator = data1.iterator
    val splitIterators = SplitIterator.split[(Int, String)](4, iterator, _._1)
    assert(iterator.hasNext)
    val split = splitIterators.map(_.toList.map(_._2))
    assert(!iterator.hasNext)

    split(0) should equal(Seq("a", "b", "f", "m", "n", "o"))
    split(1) should equal(Seq("g", "h", "l"))
    split(2) should equal(Seq("d", "j"))
    split(3) should equal(Seq("c", "e", "i", "k"))

  }

  test("test split iterator head") {
    val split = SplitIterator.split[(Int, String)](4, data1.iterator, _._1).map(_.head._2)
    split should equal(Seq("a", "g", "d", "c"))
  }

  test("test split iterator hasNext") {
    val iterator = data1.iterator
    val iterators = SplitIterator.split[(Int, String)](5, iterator, _._1)
    assert(iterator.hasNext)
    val nexts = iterators.map(_.hasNext)
    nexts should equal(Seq(true, true, true, true, false))

    assert(!iterator.hasNext) // should have forced a buffering of all elements.
    val split = iterators.map(_.toList.map(_._2))
    split(0) should equal(Seq("a", "b", "f", "m", "n", "o"))
    split(1) should equal(Seq("g", "h", "l"))
    split(2) should equal(Seq("d", "j"))
    split(3) should equal(Seq("c", "e", "i", "k"))
  }
}
