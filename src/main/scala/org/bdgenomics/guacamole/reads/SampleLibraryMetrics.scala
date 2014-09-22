package org.bdgenomics.guacamole.reads

import org.bdgenomics.guacamole.pileup.Pileup

/**
 *
 * SampleLibraryMetrics stores aggregates statistics about the reads in a sample
 *
 * @param numReads Number of reads in the sample (NOTE: This will over count the number of reads processed which will be
 *                 larger than the number in th reads in the sample since some of duplicated during partitioning
 * @param numLoci Number of loci in the sample
 * @param totalInsertSize Total sum of the paired-reads insert sizes
 * @param totalDepth Total number of reads at each loci
 */
case class SampleLibraryMetrics(  var numReads: Long = 0L,
                                  var numLoci: Long = 0L,
                                  var totalInsertSize: Long = 0L,
                                  var totalDepth: Long = 0L) {



  def averageReadDepth: Float = totalDepth / numLoci.toFloat
  def averageInsertSize: Float = totalInsertSize / numReads.toFloat

  def +(other: SampleLibraryMetrics): SampleLibraryMetrics = {
    numReads += other.numReads
    numLoci += other.numLoci
    totalInsertSize += other.totalInsertSize
    totalDepth += other.totalDepth

    this
  }

  def insertReads(reads: Seq[MappedRead]) = {
    val numNewReads = reads.size
    totalInsertSize += reads.flatMap(_.matePropertiesOpt.flatMap(_.inferredInsertSize)).sum
    numReads += numNewReads
  }

  def insertPileup(pileup: Pileup) = {
    insertReadDepth(pileup.depth)
  }

  def insertReadDepth(depth: Int) = {
    totalDepth += depth
    numLoci += 1L
  }
}

object SampleLibraryMetrics {

  val SampleLibraryMetricsZero = SampleLibraryMetrics()
}
