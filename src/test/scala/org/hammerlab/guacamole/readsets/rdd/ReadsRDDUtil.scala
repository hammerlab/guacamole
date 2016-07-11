package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.ReadsUtil
import org.hammerlab.guacamole.readsets.loading.InputFilters
import org.hammerlab.guacamole.readsets.{PerSample, ReadSets}
import org.hammerlab.guacamole.reference.Position.Locus
import org.hammerlab.guacamole.reference.{Contig, ReferenceRegion}

import scala.collection.mutable

case class TestRegion(contig: Contig, start: Long, end: Long) extends ReferenceRegion

class DebugIterator[T](it: Iterator[T]) extends Iterator[T] {

  val className = it.getClass.getSimpleName

  override def hasNext: Boolean = {
    println(s"$className: begin hasNext")
    val b = it.hasNext
    println(s"$className: end hasNext: $b")
    b
  }

  override def next(): T = {
    println(s"$className: begin next:")
    val n = it.next()
    println(s"$className: end next: $n")
    n
  }
}

trait ReadsRDDUtil extends ReadsUtil {

  def sc: SparkContext

  def makeReadsRDD(numPartitions: Int, reads: (String, Int, Int, Int)*): RDD[TestRegion] =
    sc.parallelize(makeReads(reads).toSeq, numPartitions)

  def makeReadSets(paths: PerSample[String], loci: LociParser): ReadSets =
    ReadSets(sc, paths.zip(paths), filters = InputFilters(overlapsLoci = loci))
}

object ReadsRDDUtil {
  type TestPileup = (Contig, Locus, String)

  // This is used as a closure in `RDD.map`s in ReadSetsSuite, so it's advantageous to keep it in its own object here
  // and not serialize ReadSetsSuite (which is not Serializable due to scalatest Matchers' `assertionsHelper`).
  def simplifyPileup(pileup: Pileup): TestPileup =
    (
      pileup.contig,
      pileup.locus,
      {
        val reads = mutable.Map[(Long, Long), Int]()
        for {
          e <- pileup.elements
          read = e.read
          contig = read.contig
          start = read.start
          end = read.end
        } {
          reads((start, end)) = reads.getOrElseUpdate((start, end), 0) + 1
        }

        (for {
          ((start, end), count) <- reads.toArray.sortBy(_._1._2)
        } yield
          s"[$start,$end)${if (count > 1) s"*$count" else ""}"
        ).mkString(", ")
      }
    )
}
