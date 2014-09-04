package org.bdgenomics.guacamole.variants

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.formats.avro.{ Genotype, GenotypeAllele }

import scala.collection.JavaConversions

object CalledGenotype {

  implicit def calledGenotypeToADAMGenotype(calledGenotype: CalledGenotype): Seq[Genotype] = {
    Seq(Genotype.newBuilder
      .setAlleles(JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
      .setSampleId(calledGenotype.sampleName.toCharArray)
      .setGenotypeQuality(calledGenotype.evidence.phredScaledLikelihood)
      .setReadDepth(calledGenotype.evidence.readDepth)
      .setExpectedAlleleDosage(calledGenotype.evidence.alternateReadDepth.toFloat / calledGenotype.evidence.readDepth)
      .setAlternateReadDepth(calledGenotype.evidence.alternateReadDepth)
      .setVariant(calledGenotype.adamVariant)
      .build)
  }

  implicit def calledSomaticGenotypeToADAMGenotype(calledGenotype: CalledSomaticGenotype): Seq[Genotype] = {
    Seq(Genotype.newBuilder
      .setAlleles(JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
      .setSampleId(calledGenotype.sampleName.toCharArray)
      .setGenotypeQuality(calledGenotype.tumorEvidence.phredScaledLikelihood)
      .setReadDepth(calledGenotype.tumorEvidence.readDepth)
      .setExpectedAlleleDosage(calledGenotype.tumorEvidence.alternateReadDepth.toFloat / calledGenotype.tumorEvidence.readDepth)
      .setAlternateReadDepth(calledGenotype.tumorEvidence.alternateReadDepth)
      .setVariant(calledGenotype.adamVariant)
      .build)
  }
}

case class CalledGenotype(sampleName: String,
                          referenceContig: String,
                          start: Long,
                          referenceBase: Byte,
                          alternateBase: Seq[Byte],
                          evidence: GenotypeEvidence,
                          length: Int = 1) extends ReferenceVariant {
  val end: Long = start + 1L

}

class CalledGenotypeSerializer extends Serializer[CalledGenotype] {

  lazy val genotypeEvidenceSeralizer = new GenotypeEvidenceSerializer()

  def write(kryo: Kryo, output: Output, obj: CalledGenotype) = {
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeLong(obj.start, true)
    output.writeByte(obj.referenceBase)
    output.writeInt(obj.alternateBase.length, true)
    output.writeBytes(obj.alternateBase.toArray)
    genotypeEvidenceSeralizer.write(kryo, output, obj.evidence)
    output.writeInt(obj.length, true)

  }

  def read(kryo: Kryo, input: Input, klass: Class[CalledGenotype]): CalledGenotype = {

    val sampleName: String = input.readString()
    val referenceContig: String = input.readString()
    val start: Long = input.readLong(true)
    val referenceBase: Byte = input.readByte()
    val alternateLength = input.readInt(true)
    val alternateBase: Seq[Byte] = input.readBytes(alternateLength)

    val evidence = genotypeEvidenceSeralizer.read(kryo, input, classOf[GenotypeEvidence])
    val length: Int = input.readInt(true)

    CalledGenotype(
      sampleName,
      referenceContig,
      start,
      referenceBase,
      alternateBase,
      evidence,
      length
    )

  }
}

case class CalledSomaticGenotype(sampleName: String,
                                 referenceContig: String,
                                 start: Long,
                                 referenceBase: Byte,
                                 alternateBase: Seq[Byte],
                                 somaticLogOdds: Double,
                                 tumorEvidence: GenotypeEvidence,
                                 normalEvidence: GenotypeEvidence,
                                 length: Int = 1) extends ReferenceVariant {
  val end: Long = start + 1L
}

class CalledSomaticGenotypeSerializer extends Serializer[CalledSomaticGenotype] {

  lazy val genotypeEvidenceSerializer = new GenotypeEvidenceSerializer()

  def write(kryo: Kryo, output: Output, obj: CalledSomaticGenotype) = {
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeLong(obj.start)
    output.writeByte(obj.referenceBase)
    output.writeInt(obj.alternateBase.length, true)
    output.writeBytes(obj.alternateBase.toArray)
    output.writeDouble(obj.somaticLogOdds)

    genotypeEvidenceSerializer.write(kryo, output, obj.tumorEvidence)
    genotypeEvidenceSerializer.write(kryo, output, obj.normalEvidence)

    output.writeInt(obj.length, true)

  }

  def read(kryo: Kryo, input: Input, klass: Class[CalledSomaticGenotype]): CalledSomaticGenotype = {

    val sampleName: String = input.readString()
    val referenceContig: String = input.readString()
    val start: Long = input.readLong()
    val referenceBase: Byte = input.readByte()
    val alternateLength = input.readInt(true)
    val alternateBase = input.readBytes(alternateLength).toSeq
    val somaticLogOdds = input.readDouble()

    val tumorEvidence = genotypeEvidenceSerializer.read(kryo, input, classOf[GenotypeEvidence])
    val normalEvidence = genotypeEvidenceSerializer.read(kryo, input, classOf[GenotypeEvidence])

    val length: Int = input.readInt(true)

    CalledSomaticGenotype(
      sampleName,
      referenceContig,
      start,
      referenceBase,
      alternateBase,
      somaticLogOdds,
      tumorEvidence = tumorEvidence,
      normalEvidence = normalEvidence
    )

  }

}

