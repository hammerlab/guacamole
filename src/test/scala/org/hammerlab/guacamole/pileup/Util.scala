package org.hammerlab.guacamole.pileup

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.io.InputFilters
import org.hammerlab.guacamole.readsets.rdd.ReadsRDDUtil
import org.hammerlab.guacamole.reference.{ContigName, Locus, ReferenceBroadcast}

trait Util
  extends ReadsRDDUtil {

  def reference: ReferenceBroadcast

  def makePileup(reads: Seq[MappedRead],
                 contigName: ContigName,
                 locus: Locus) =
    Pileup(reads, contigName, locus, reference.getContig(contigName))

  def loadTumorNormalPileup(tumorReads: Seq[MappedRead],
                            normalReads: Seq[MappedRead],
                            locus: Locus): (Pileup, Pileup) = {
    val contig = tumorReads(0).contigName
    assume(normalReads(0).contigName == contig)
    (
      Pileup(tumorReads.filter(_.overlapsLocus(locus)), contig, locus, reference.getContig(contig)),
      Pileup(normalReads.filter(_.overlapsLocus(locus)), contig, locus, reference.getContig(contig))
    )
  }

  def loadPileup(sc: SparkContext,
                 filename: String,
                 locus: Locus = 0,
                 maybeContig: Option[ContigName] = None,
                 reference: ReferenceBroadcast = reference): Pileup = {
    val records =
      loadReadsRDD(
        sc,
        filename,
        filters = InputFilters(
          overlapsLoci = maybeContig.map(
            contig => LociParser(s"$contig:$locus-${locus + 1}")
          ).orNull
        )
      ).mappedReads

    val localReads = records.collect

    val actualContig = maybeContig.getOrElse(localReads(0).contigName)

    Pileup(
      localReads,
      actualContig,
      locus,
      reference.getContig(actualContig)
    )
  }
}
