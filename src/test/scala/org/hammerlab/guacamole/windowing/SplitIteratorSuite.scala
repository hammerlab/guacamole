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
    val splitIterators = SplitIterator.split[String](4, iterator)
    assert(iterator.hasNext)
    val split = splitIterators.map(_.toList)
    assert(!iterator.hasNext)

    split(0) should equal(Seq("a", "b", "f", "m", "n", "o"))
    split(1) should equal(Seq("g", "h", "l"))
    split(2) should equal(Seq("d", "j"))
    split(3) should equal(Seq("c", "e", "i", "k"))

  }

  test("test split iterator head") {
    val split = SplitIterator.split[String](4, data1.iterator).map(_.head)
    split should equal(Seq("a", "g", "d", "c"))
  }

  test("test split iterator hasNext") {
    val iterator = data1.iterator
    val iterators = SplitIterator.split[String](5, iterator)
    assert(iterator.hasNext)
    val nexts = iterators.map(_.hasNext)
    nexts should equal(Seq(true, true, true, true, false))

    assert(!iterator.hasNext) // should have forced a buffering of all elements.
    val split = iterators.map(_.toList)
    split(0) should equal(Seq("a", "b", "f", "m", "n", "o"))
    split(1) should equal(Seq("g", "h", "l"))
    split(2) should equal(Seq("d", "j"))
    split(3) should equal(Seq("c", "e", "i", "k"))
  }
}
