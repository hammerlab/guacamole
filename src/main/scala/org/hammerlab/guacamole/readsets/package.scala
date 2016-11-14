package org.hammerlab.guacamole

import org.apache.spark.rdd.RDD
import org.hammerlab.genomics.reference.{ContigName, NumLoci}
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.ContigLengths
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegions

/**
 * This package contains functionality related to processing multiple "sets of reads" (e.g. BAM files) in the context of
 * a single analysis.
 *
 * For example, a standard somatic caller will typically load and analyze separate "normal" and "tumor" samples, each
 * corresponding to an [[RDD[MappedRead]]], but which will also share metadata, like the [[ContigLengths]] of the
 * reference they are mapped to.
 */
package object readsets {
  type PerSample[+A] = IndexedSeq[A]

  type ContigLengths = Map[ContigName, NumLoci]

  type SampleId = Int
  type NumSamples = Int

  type SampleName = String

  type PartitionedReads = PartitionedRegions[MappedRead]
}
