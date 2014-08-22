package org.bdgenomics.guacamole.reads

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import net.sf.samtools.TextCigarCodec

// Serialization: MappedRead
class MappedReadSerializer extends Serializer[MappedRead] with CanSerializeMatePropertiesOption {

  def write(kryo: Kryo, output: Output, obj: MappedRead) = {
    output.writeInt(obj.token)
    assert(obj.sequence.length == obj.baseQualities.length)
    output.writeInt(obj.sequence.length, true)
    output.writeBytes(obj.sequence)
    output.writeBytes(obj.baseQualities)
    output.writeBoolean(obj.isDuplicate)
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeInt(obj.alignmentQuality, true)
    output.writeLong(obj.start, true)
    output.writeString(obj.cigar.toString)
    obj.mdTagStringOpt match {
      case None =>
        output.writeBoolean(false)
      case Some(tag) =>
        output.writeBoolean(true)
        output.writeString(tag)
    }
    output.writeBoolean(obj.failedVendorQualityChecks)
    output.writeBoolean(obj.isPositiveStrand)

    write(kryo, output, obj.matePropertiesOpt)
  }

  def read(kryo: Kryo, input: Input, klass: Class[MappedRead]): MappedRead = {
    val token = input.readInt()
    val count: Int = input.readInt(true)
    val sequenceArray: Array[Byte] = input.readBytes(count)
    val qualityScoresArray = input.readBytes(count)
    val isDuplicate = input.readBoolean()
    val sampleName = input.readString().intern()
    val referenceContig = input.readString().intern()
    val alignmentQuality = input.readInt(true)
    val start = input.readLong(true)
    val cigarString = input.readString()
    val hasMdTag = input.readBoolean()
    val mdTagStringOpt = if (hasMdTag) Some(input.readString()) else None
    val failedVendorQualityChecks = input.readBoolean()
    val isPositiveStrand = input.readBoolean()

    val matePropertiesOpt = read(kryo, input)

    val cigar = TextCigarCodec.getSingleton.decode(cigarString)
    MappedRead(
      token,
      sequenceArray,
      qualityScoresArray,
      isDuplicate,
      sampleName.intern,
      referenceContig,
      alignmentQuality,
      start,
      cigar,
      mdTagStringOpt,
      failedVendorQualityChecks,
      isPositiveStrand,
      matePropertiesOpt
    )
  }
}

