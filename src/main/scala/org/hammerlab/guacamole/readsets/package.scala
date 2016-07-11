package org.hammerlab.guacamole

import org.hammerlab.guacamole.reference.Contig
import org.hammerlab.guacamole.reference.Position.NumLoci

/**
 * Convenience types for loading in sets of reads.
 */
package object readsets {
  type PerSample[A] = IndexedSeq[A]
  type ContigLengths = Map[Contig, NumLoci]
}
