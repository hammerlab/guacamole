package org.hammerlab.guacamole.reference

trait ContigSequence {
  def apply(locus: Locus): Byte
  def slice(start: Locus, end: Locus): Array[Byte]
  def length: NumLoci
}
