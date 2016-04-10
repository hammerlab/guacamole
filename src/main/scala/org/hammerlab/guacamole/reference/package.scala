package org.hammerlab.guacamole

package object reference {
  // Denotes a sequence of bytes that specifically represents a full reference contig.
  type ContigSequence = IndexedSeq[Byte]

  // Denotes a sequence of bytes that is a subsequence of a contig.
  type ContigSubsequence = IndexedSeq[Byte]
}
