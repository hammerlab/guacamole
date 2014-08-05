package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole._
import scala.Serializable
import org.apache.spark.Logging
import org.bdgenomics.guacamole.Common.Arguments.{ TumorNormalReads, Reads, Output, Base }
import org.kohsuke.args4j.{ Option => Opt }
import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.{ ADAMGenotypeAllele, ADAMContig, ADAMVariant, ADAMGenotype }
import scala.Some
import org.bdgenomics.guacamole.pileup.Pileup
import scala.collection.JavaConversions

case class Breakpoint(referenceContig: String, referenceBase: Byte, start: Long, end: Long, evidence: Int) extends Serializable {

  lazy val length = end - start + 1
  lazy val midpoint = (start + end) / 2.0

}

object AlignmentRuleSVCaller extends Command with Serializable with Logging {

  override val name = "alignment-sv"
  override val description = "call structural variants using a simple alignment rules"

  private class Arguments extends Base with Output with TumorNormalReads with DistributedUtil.Arguments {
    @Opt(name = "-threshold", metaVar = "X", usage = "Make a call if at least X% of reads support it. Default: 8")
    var threshold: Int = 8

    @Opt(name = "-window", metaVar = "X", usage = "Size of window to look at ")
    var windowSize: Int = 50
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    val filters = Read.InputFilters(mapped = true, nonDuplicate = true, hasMdTag = true)
    val (tumorReads, normalReads) = Common.loadTumorNormalReadsFromArguments(args, sc, filters)

    Common.progress("Loaded %,d tumor mapped non-duplicate MdTag-containing reads into %,d partitions.".format(
      tumorReads.mappedReads.count, tumorReads.mappedReads.partitions.length))
    Common.progress("Loaded %,d normal mapped non-duplicate MdTag-containing reads into %,d partitions.".format(
      normalReads.mappedReads.count, normalReads.mappedReads.partitions.length))

    val loci = Common.loci(args, normalReads)

    val windowSize = args.windowSize
    val minThreshold = args.threshold
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, tumorReads.mappedReads, normalReads.mappedReads)
    val genotypes: RDD[ADAMGenotype] = DistributedUtil.windowFlatMapWithState[ADAMGenotype, Option[Breakpoint]](
      Seq(tumorReads.mappedReads,
        normalReads.mappedReads),
      lociPartitions,
      true, // skip empty windows.
      windowSize, //half window size, gives 2x + 1 base-pair window
      None,
      (lastBreakpoint: Option[Breakpoint], windows: Seq[SlidingReadWindow]) => {
        val window = windows(0)
        val (breakpoint, genotypes) = findBreakpointAtLocus(window, lastBreakpoint, minThreshold)
        (breakpoint, genotypes.iterator)
      })
    Common.writeVariantsFromArguments(args, genotypes)
  }

  /**
   * Converts a breakpoint into an ADAMGenotype
   * @param breakpoint Discovered breakpoint in some genomic region
   * @return ADAMGenotype filled as structural variant
   */
  def buildVariant(breakpoint: Breakpoint): ADAMGenotype = {
    val allelesList = List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)
    ADAMGenotype.newBuilder
      .setSampleId("somatic".toCharArray)
      .setAlleles(JavaConversions.seqAsJavaList(allelesList))
      .setVariant(ADAMVariant.newBuilder
        .setPosition(breakpoint.start)
        .setReferenceAllele(Bases.baseToString(breakpoint.referenceBase))
        .setVariantAllele("<SV>")
        .setContig(ADAMContig.newBuilder.setContigName(breakpoint.referenceContig).build)
        .build)
      .build
  }

  /**
   * Takes two breakpoints and creates a single breakpoint that connects them
   * @param lastBreakpoint The first breakpoint in the genomic region
   * @param newBreakPoint The second breakpoint in the genomic region
   * @return A single connected breakpoint
   */
  def extendBreakpoint(lastBreakpoint: Breakpoint, newBreakPoint: Breakpoint): Breakpoint = {
    Breakpoint(lastBreakpoint.referenceContig,
      lastBreakpoint.referenceBase,
      lastBreakpoint.start,
      newBreakPoint.end,
      lastBreakpoint.evidence + newBreakPoint.evidence)
  }

  /**
   *
   * Finds a breakpoint that may cause a structural variant from a window of reads
   * If there is a previously found breakpoint and another is found, the breakpoint is extended
   * If there is a previously found breakpoint and another is *not* found, we emit a variant from the previous one
   *
   * A variant is only emitted once we reach the end of a breakpoint (we don't find any more breakpoints)
   *
   * @param window Current window of reads to analyze
   * @param lastBreakpoint Any previously found breakpoint in the genomic region (default: None)
   * @param threshold minimum percent of reads needed to support the variant (default: 0)
   * @return
   */
  def findBreakpointAtLocus(window: SlidingReadWindow,
                            lastBreakpoint: Option[Breakpoint] = None,
                            threshold: Int = 0): (Option[Breakpoint], Seq[ADAMGenotype]) = {
    val breakpointCandidate = discoverBreakpoint(window.currentReads().map(_.getMappedRead()))
    (lastBreakpoint, breakpointCandidate) match {
      case (Some(last), Some(breakpoint)) => (Some(extendBreakpoint(last, breakpoint)), Seq.empty)
      case (Some(last), None)             => (None, Seq(buildVariant(last)))
      case (None, Some(breakpoint))       => (Some(breakpoint), Seq.empty)
      case (None, None)                   => (None, Seq.empty)
    }
  }

  /**
   *
   * This variant caller relies on mated pair-end information to call structural variants.
   *
   * In mated paired end, we expect the following pair structure
   *
   *  r1 -------> <-------- r2
   *  Where the read with earlier start position is oriented positively 5' -> 3' and its mate (which starts later)
   *  *must* be oriented negatively.
   *
   *  Translocation
   *  Reads mapped to different chromosomes
   *  r1 -------> ..... (????)
   *
   *  Inversions:
   *  Reads mapped in the same directions
   *  r1 -------> --------> r2 or r1 <------- <-------- r2
   *
   *  Tandem Duplications:
   *  Reads mapped in the incorrect directions
   *  r1 <------- --------> r2
   */

  def discoverBreakpoint(reads: Seq[MappedRead], minThreshold: Int = 0): Option[Breakpoint] = {

    def hasBreakpoint(read: MappedRead): Boolean = {
      def isTranslocation(read: MappedRead): Boolean = {
        (read.isMapped && !read.isMateMapped) || read.mateReferenceContig.exists(_ != read.referenceContig)
      }

      def isInversion(read: MappedRead): Boolean = {
        read.mateReferenceContig.exists(_ == read.referenceContig) && read.isPositiveStrand == read.isMatePositiveStrand
      }

      def isDuplication(read: MappedRead): Boolean = {
        read.isMateMapped && ((read.isFirstInPair && !read.isPositiveStrand) || (!read.isFirstInPair && read.isPositiveStrand))
      }

      isDuplication(read) || isInversion(read) || isTranslocation(read)
    }

    val variantReads = reads.view.filter(hasBreakpoint)
    val numBreakpointReads = variantReads.size
    if (numBreakpointReads > 0 && 100.0 * numBreakpointReads / reads.size > minThreshold) {
      val breakpointContig = variantReads.head.referenceContig
      val possibleStarts = variantReads.view.map(read => (read.sequence.head, read.start))
      val breakpointStart = possibleStarts.minBy(_._2)
      val breakpointEnd = variantReads.view.map(r => r.mateStart.getOrElse(r.end)).max
      Some(Breakpoint(breakpointContig, breakpointStart._1, breakpointStart._2, breakpointEnd, numBreakpointReads))
    } else {
      None
    }
  }
}
