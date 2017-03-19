package org.hammerlab.guacamole

import org.hammerlab.genomics.reads.MappedRead
import org.hammerlab.genomics.readsets.SampleRead
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegions

/**
 * This package contains functionality related to processing multiple "sets of reads" (e.g. BAM files) in the context of
 * a single analysis.
 */
package object readsets {
  type PartitionedReads = PartitionedRegions[SampleRead, MappedRead]
}
