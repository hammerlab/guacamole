package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.Bases

object ReferenceUtils {

  def getBaseRatios(sequence: Array[Byte]): Map[Byte, Float] = {
    val sequenceLength = sequence.length.toFloat
    sequence.groupBy(identity).mapValues(_.length / sequenceLength).withDefaultValue(0f)
  }

  def getGCRatio(sequence: Array[Byte]): Float = {
    val baseRatios = getBaseRatios(sequence)
    baseRatios(Bases.G) + baseRatios(Bases.C)
  }

  def getATRatio(sequence: Array[Byte]): Float = {
    val baseRatios = getBaseRatios(sequence)
    baseRatios(Bases.A) + baseRatios(Bases.T)
  }

}
