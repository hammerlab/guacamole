package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.apache.spark.util.StatCounter
import org.hammerlab.guacamole.{ DistributedUtil, SparkCommand, Common }
import org.hammerlab.guacamole.reads.{ PairedRead, Read, MappedRead }
import org.kohsuke.args4j.{ Option => Args4JOption }

import scala.collection.mutable.ArrayBuffer

/**
 * Structural Variant caller
 *
 * This currently looks for regions with abnormally large insert sizes. This typically indicates
 * that there's been a large deletion.
 *
 * It does this by finding the average insert size of all mate pairs which overlap each 25-base pair "block" of the
 * genome. If this average is significantly different than the average insert size genome-wide, then it's evidence that
 * there may be a structural variant.
 *
 * The output is a text file of genomic ranges where there's some evidence of an SV.
 */
object StructuralVariant {

  val MAX_INSERT_SIZE = 25000
  val BLOCK_SIZE = 25

  protected class Arguments extends Common.Arguments.Reads with DistributedUtil.Arguments {
    @Args4JOption(name = "--filter-contig", usage = "Filter to alignments where either mate is in this contig.")
    var filterContig: String = ""

    @Args4JOption(name = "--output", usage = "Path to output text file.")
    var output: String = ""
  }

  case class Location(
      contig: String,
      position: Long) {
  }

  case class GenomeRange(
    contig: String,
    start: Long,
    stop: Long)

  object Caller extends SparkCommand[Arguments] {
    override val name = "structural-variant"
    override val description = "Find structural variants, e.g. large deletions."

    // The sign of inferredInsertSize follows the orientation of the read. This returns
    // an insert size which is positive in the common case, regardless of the orientation.
    def orientedInsertSize(r: PairedRead[MappedRead]): Option[Int] = {
      val sgn = if (r.isPositiveStrand) { 1 } else { -1 }
      r.mateAlignmentProperties.flatMap(_.inferredInsertSize).map(x => x * sgn)
    }

    // Coalesce sequences of values separated by blockSize into single (inclusive) ranges.
    // The sequence of values must be sorted and consist of multiples of blockSize.
    // For example, (0, 10, 20, 30, 70, 80) --> [(0, 30), (70, 80)]
    def coalesceAdjacent(xs: IndexedSeq[Long], blockSize: Long): Iterator[(Long, Long)] = {
      val rangeStarts = ArrayBuffer(0)
      for (i <- 1 until xs.length) {
        if (xs(i - 1) + blockSize != xs(i)) {
          rangeStarts += i
        }
      }
      rangeStarts += xs.length

      for {
        Seq(startIdx, nextStartIdx) <- rangeStarts.sliding(2)
      } yield {
        (xs(startIdx), xs(nextStartIdx - 1))
      }
    }

    // Returns a sequence of all blocks which overlap the read, its mate or the insert between them.
    def matedReadBlocks(r: PairedRead[MappedRead]): Seq[Long] = {
      if (!r.isMateMapped) {
        Seq[Long]()
      } else {
        val mateStart = r.mateAlignmentProperties.map(mp => mp.start).getOrElse(r.read.start)
        val start = Math.min(r.read.start, mateStart)
        val roundedStart = start / BLOCK_SIZE * BLOCK_SIZE
        val insertSize = orientedInsertSize(r).getOrElse(0)
        roundedStart to (roundedStart + insertSize) by BLOCK_SIZE
      }
    }

    override def run(args: Arguments, sc: SparkContext): Unit = {
      val readSet = Common.loadReadsFromArguments(args, sc, Read.InputFilters(nonDuplicate = true))
      val pairedMappedReads = readSet.mappedPairedReads
      val firstInPair = pairedMappedReads.filter(_.isFirstInPair)

      // Pare down to mate pairs which aren't highly displaced & aren't inverted.
      val readsInRange = for {
        r <- firstInPair
        s <- orientedInsertSize(r)
        if 0 < s && s < MAX_INSERT_SIZE
      } yield r
      readsInRange.persist()

      val insertSizes = readsInRange.flatMap(orientedInsertSize)
      val insertStats = insertSizes.stats()
      println("Stats on inferredInsertSize: " + insertStats)

      // Find blocks where the average insert size is 2 sigma from the average for the whole genome.
      val exceptionalThreshold = insertStats.mean + 2 * insertStats.stdev
      val exceptionalBlocks = (for {
        r <- readsInRange
        pos <- matedReadBlocks(r)
        insertSize <- orientedInsertSize(r)
      } yield {
        Location(r.read.referenceContig, pos) -> insertSize
      })
        .aggregateByKey(new StatCounter())(seqOp = _.merge(_), combOp = _.merge(_))
        .filter { case (loc, stats) => stats.mean > exceptionalThreshold }
        .map(_._1)

      // Group adjacent blocks on the same contig
      val exceptionalRanges = exceptionalBlocks
        .groupBy(_.contig)
        .flatMap {
          case (contig, locations) => {
            val positions = locations.map(_.position).toArray.sorted
            coalesceAdjacent(positions, BLOCK_SIZE)
              .map(x => GenomeRange(contig, x._1, x._2))
          }
        }

      println("Number of exceptional blocks: " + exceptionalBlocks.count())
      exceptionalRanges.coalesce(1).saveAsTextFile(args.output)
    }
  }
}
