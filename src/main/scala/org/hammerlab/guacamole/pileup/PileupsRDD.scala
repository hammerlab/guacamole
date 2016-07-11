package org.hammerlab.guacamole.pileup

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.PartitionedReads
import org.hammerlab.guacamole.readsets.iterator.overlaps.PositionRegionsIterator
import org.hammerlab.guacamole.readsets.{NumSamples, PerSample}
import org.hammerlab.guacamole.reference.{Position, ReferenceGenome}

class PileupsRDD(partitionedReads: PartitionedReads) {

  def sc = partitionedReads.sc
  def lociSetsRDD = partitionedReads.lociSetsRDD

  def pileups(reference: ReferenceGenome,
              forceCallLoci: LociSet = LociSet()): RDD[Pileup] = {

    val forceCallLociBroadcast: Broadcast[LociSet] = sc.broadcast(forceCallLoci)

    assert(
      partitionedReads.regions.getNumPartitions == lociSetsRDD.getNumPartitions,
      s"reads partitions: ${partitionedReads.regions.getNumPartitions}, loci partitions: ${lociSetsRDD.getNumPartitions}"
    )

    val numLoci = sc.accumulator(0L, "numLoci")

    partitionedReads.regions.zipPartitions(lociSetsRDD, preservesPartitioning = true)((reads, lociIter) => {
      val loci = lociIter.next()
      if (lociIter.hasNext) {
        throw new Exception(s"Expected 1 LociSet, found ${1 + lociIter.size}.\n$loci")
      }

      numLoci += loci.count

      val windowedReads =
        new PositionRegionsIterator(
          halfWindowSize = 0,
          loci,
          forceCallLociBroadcast.value,
          reads.buffered
        )

      for {
        (Position(contig, locus), reads) <- windowedReads
      } yield {
        Pileup(reads, contig, locus, reference.getContig(contig))
      }
    })
  }

  def perSamplePileups(numSamples: NumSamples,
                       reference: ReferenceGenome,
                       forceCallLoci: LociSet = LociSet()): RDD[PerSample[Pileup]] =
    pileups(reference, forceCallLoci).map(_.bySample(numSamples))
}

object PileupsRDD {
  implicit def partitionedReadsToPileupsRDD(partitionedReads: PartitionedReads): PileupsRDD =
    new PileupsRDD(partitionedReads)
}
