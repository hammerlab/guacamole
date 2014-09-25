package org.bdgenomics.guacamole.reads

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import htsjdk.samtools.TextCigarCodec

// Serialization: MappedRead
class MappedReadSerializer extends Serializer[MappedRead] with CanSerializeMatePropertiesOption {

  def write(kryo: Kryo, output: Output, obj: MappedRead) = {
    output.writeInt(obj.token)
    assert(obj.sequence.length == obj.baseQualities.length)
    output.writeInt(obj.sequence.length, true)
    output.writeBytes(obj.sequence.toArray)
    output.writeBytes(obj.baseQualities.toArray)
    output.writeBoolean(obj.isDuplicate)
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeInt(obj.alignmentQuality, true)
    output.writeLong(obj.start, true)
    output.writeString(obj.cigar.toString)
    output.writeString(obj.mdTagString)
    output.writeBoolean(obj.failedVendorQualityChecks)
    output.writeBoolean(obj.isPositiveStrand)

    write(kryo, output, obj.matePropertiesOpt)
  }

  def read(kryo: Kryo, input: Input, klass: Class[MappedRead]): MappedRead = {
    val token = input.readInt()
    val count: Int = input.readInt(true)
    val sequenceArray: Seq[Byte] = input.readBytes(count)
    val qualityScoresArray: Seq[Byte] = input.readBytes(count)
    val isDuplicate = input.readBoolean()
    val sampleName = input.readString().intern()
    val referenceContig = input.readString().intern()
    val alignmentQuality = input.readInt(true)
    val start = input.readLong(true)
    val cigarString = input.readString()
    val mdTagString = input.readString()
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
      mdTagString,
      failedVendorQualityChecks,
      isPositiveStrand,
      matePropertiesOpt
    )
  }
}

