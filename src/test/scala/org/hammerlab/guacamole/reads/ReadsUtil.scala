package org.hammerlab.guacamole.reads

import org.hammerlab.guacamole.reference.{Interval, Position, TestInterval}
import org.hammerlab.magic.iterator.RunLengthIterator
import org.scalatest.Matchers

import scala.collection.SortedMap
import scala.reflect.ClassTag

trait ReadsUtil extends Matchers {
  def checkReads[I <: Interval: ClassTag](pileups: Iterator[(Position, Iterable[I])],
                                          expectedStrs: Map[(String, Int), String]): Unit = {

    val expected = expectedStrs.map(t => Position(t._1._1, t._1._2) -> t._2)

    val actual: List[(Position, String)] = windowIteratorStrings(pileups)
    val actualMap = SortedMap(actual: _*)

    val extraLoci =
      (for {
        (k, v) <- actualMap
        if !expected.contains(k)
      } yield
        k -> v
      )
      .toArray
      .sortBy(x => x)

    val missingLoci =
      (for {
        (k, v) <- expected
        if !actualMap.contains(k)
      } yield
        k -> v
      )
      .toArray
      .sortBy(x => x)

    val msg =
      (
        List(
          s"expected ${expected.size} loci."
        ) ++
          (
            if (extraLoci.nonEmpty)
              List(
                s"${extraLoci.length} extra loci found:",
                s"\t${extraLoci.mkString("\n\t")}"
              )
            else
              Nil
          ) ++
          (
            if (missingLoci.nonEmpty)
              List(
                s"${missingLoci.length} missing loci:",
                s"\t${missingLoci.mkString("\n\t")}"
              )
            else
              Nil
          )
        ).mkString("\n")

    withClue(msg) {
      missingLoci.length should be(0)
      extraLoci.length should be(0)
    }

    val incorrectLoci =
      (for {
        (k, e) <- expected
        a <- actualMap.get(k)
        if a != e
      } yield
        k -> (a, e)
      )
      .toArray
      .sortBy(_._1)

    val incorrectLociStr =
      (for {
        (k, (a, e)) <- incorrectLoci
      } yield
        s"$k:\tactual: $a\texpected: $e"
      ).mkString("\n")

    withClue(s"differing loci:\n$incorrectLociStr") {
      incorrectLoci.length should be(0)
    }
  }

  def windowIteratorStrings[I <: Interval: ClassTag](
    windowIterator: Iterator[(Position, Iterable[I])]
  ): List[(Position, String)] =
    (for {
      (pos, reads) <- windowIterator
    } yield {
      pos ->
        (for {
          (region, count) <- RunLengthIterator(reads.toArray.sortBy(_.start).iterator)
        } yield {
          s"[${region.start},${region.end})${if (count > 1) s"*$count" else ""}"
        }).mkString(", ")
    }).toList

  def makeReads(reads: Seq[(String, Int, Int, Int)]): BufferedIterator[TestRegion] =
    (for {
      (contig, start, end, num) <- reads
      i <- 0 until num
    } yield
      TestRegion(contig, start, end)
    ).iterator.buffered
}
