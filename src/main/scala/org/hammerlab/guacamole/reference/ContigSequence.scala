package org.hammerlab.guacamole.reference

trait ContigSequence {
  def apply(index: Int): Byte
  def slice(start: Int, end: Int): Array[Byte]
  def length: Int
}
