package org.bdgenomics.guacamole.variants

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.guacamole.pileup.Pileup

case class GenotypeEvidence(likelihood: Double,
                            readDepth: Int,
                            alternateReadDepth: Int,
                            forwardDepth: Int,
                            alternateForwardDepth: Int,
                            averageMappingQuality: Double,
                            averageBaseQuality: Double) {

  lazy val phredScaledLikelihood = PhredUtils.successProbabilityToPhred(likelihood - 1e-10)
  lazy val variantAlleleFrequency = alternateReadDepth.toFloat / readDepth
}

class GenotypeEvidenceSerializer extends Serializer[GenotypeEvidence] {
  def write(kryo: Kryo, output: Output, obj: GenotypeEvidence) = {
    output.writeDouble(obj.likelihood)
    output.writeInt(obj.readDepth)
    output.writeInt(obj.alternateReadDepth)
    output.writeInt(obj.forwardDepth)
    output.writeInt(obj.alternateForwardDepth)

    output.writeDouble(obj.averageMappingQuality)
    output.writeDouble(obj.averageBaseQuality)

  }

  def read(kryo: Kryo, input: Input, klass: Class[GenotypeEvidence]): GenotypeEvidence = {

    val likelihood = input.readDouble()
    val readDepth = input.readInt()
    val alternateReadDepth = input.readInt()
    val forwardDepth = input.readInt()
    val alternateForwardDepth = input.readInt()

    val averageMappingQuality = input.readDouble()
    val averageBaseQuality = input.readDouble()

    GenotypeEvidence(likelihood,
      readDepth,
      alternateReadDepth,
      forwardDepth,
      alternateForwardDepth,
      averageMappingQuality,
      averageBaseQuality
    )

  }
}

object GenotypeEvidence {

  def apply(likelihood: Double, alternateReadDepth: Int, alternatePositiveReadDepth: Int, pileup: Pileup): GenotypeEvidence = {

    GenotypeEvidence(likelihood,
      pileup.depth,
      alternateReadDepth,
      pileup.positiveDepth,
      alternatePositiveReadDepth,
      pileup.elements.map(_.read.alignmentQuality).sum.toFloat / pileup.depth,
      pileup.elements.map(_.qualityScore).sum.toFloat / pileup.depth
    )
  }

  def apply(likelihood: Double, alternate: Seq[Byte], pileup: Pileup): GenotypeEvidence = {
    val (alternateReadDepth, alternatePositiveReadDepth) = pileup.alternateReadDepthAndPositiveDepth(alternate)
    GenotypeEvidence(likelihood, alternateReadDepth, alternatePositiveReadDepth, pileup)

  }
}
