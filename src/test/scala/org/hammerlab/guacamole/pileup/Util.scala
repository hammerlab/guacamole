package org.hammerlab.guacamole.pileup

import org.apache.spark.SparkContext
import org.hammerlab.genomics.loci.parsing.ParsedLoci
import org.hammerlab.genomics.reads.MappedRead
import org.hammerlab.genomics.readsets.io.Config
import org.hammerlab.genomics.readsets.rdd.ReadsRDDUtil
import org.hammerlab.genomics.reference.{ ContigName, Locus }
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.hadoop.MaxSplitSize
import org.hammerlab.paths.Path
import org.hammerlab.spark.test.suite.SparkSuite

trait Util
  extends ReadsRDDUtil {

  self: SparkSuite ⇒

  def reference: ReferenceBroadcast

  def makePileup(reads: Seq[MappedRead],
                 contigName: ContigName,
                 locus: Locus) =
    Pileup(reads, "sample", contigName, locus, reference(contigName))

  def makeNormalPileup(reads: Seq[MappedRead],
                       contigName: ContigName,
                       locus: Locus) =
    Pileup(reads, "normal", contigName, locus, reference(contigName))

  def makeTumorPileup(reads: Seq[MappedRead],
                      contigName: ContigName,
                      locus: Locus) =
    Pileup(reads, "tumor", contigName, locus, reference(contigName))

  def loadTumorNormalPileup(tumorReads: Seq[MappedRead],
                            normalReads: Seq[MappedRead],
                            locus: Locus): (Pileup, Pileup) = {
    val contig = tumorReads(0).contigName
    assume(normalReads(0).contigName == contig)
    (
      Pileup(tumorReads.filter(_.overlapsLocus(locus)), "tumor", contig, locus, reference(contig)),
      Pileup(normalReads.filter(_.overlapsLocus(locus)), "normal", contig, locus, reference(contig))
    )
  }

  def loadPileup(sc: SparkContext,
                 path: Path,
                 locus: Locus = Locus(0),
                 maybeContig: Option[ContigName] = None,
                 reference: ReferenceBroadcast = reference): Pileup = {
    val records =
      loadReadsRDD(
        path,
        config = Config(
          overlapsLoci = maybeContig.map(
            contig ⇒ ParsedLoci(s"$contig:$locus-${locus + 1}")
          ),
          nonDuplicate = false,
          passedVendorQualityChecks = false,
          isPaired = false,
          minAlignmentQuality = None,
          maxSplitSize = MaxSplitSize()
        )
      )
      .mappedReads

    val localReads = records.collect

    val actualContig = maybeContig.getOrElse(localReads(0).contigName)

    Pileup(
      localReads,
      path.toString,
      actualContig,
      locus,
      reference(actualContig)
    )
  }
}
