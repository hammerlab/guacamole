package org.hammerlab.guacamole

import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegions
import org.hammerlab.guacamole.reference.{ContigName, NumLoci}

/**
 * Guacamole is a framework for writing variant callers on the Apache Spark platform. Several variant callers are
 * implemented in the [[org.hammerlab.guacamole.commands]] package. The remaining packages implement a library of
 * functionality used by these callers.
 *
 * To get started, take a look at the code for [[org.hammerlab.guacamole.commands.GermlineThreshold]] for a
 * pedagogical example of a variant caller, then see [[org.hammerlab.guacamole.pileup.Pileup]] to understand how we
 * work with pileups. The [[org.hammerlab.guacamole.commands.SomaticStandard]] caller is a more
 * sophisticated caller (for the somatic setting) that gives an example of most of the functionality in the rest of the
 * library.
 */
package object readsets {
  type PerSample[+A] = IndexedSeq[A]

  type ContigLengths = Map[ContigName, NumLoci]

  type SampleId = Int
  type NumSamples = Int

  type SampleName = String

  type PartitionedReads = PartitionedRegions[MappedRead]
}
