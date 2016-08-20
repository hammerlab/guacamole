package org.hammerlab.guacamole.reads

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import htsjdk.samtools.TextCigarCodec

// Serialization: MappedRead
class MappedReadSerializer extends Serializer[MappedRead] {

  def write(kryo: Kryo, output: Output, obj: MappedRead) = {
    output.writeString(obj.name)
    assert(obj.sequence.length == obj.baseQualities.length)
    output.writeInt(obj.sequence.length, true)
    output.writeBytes(obj.sequence.toArray)
    output.writeBytes(obj.baseQualities.toArray)
    output.writeBoolean(obj.isDuplicate)
    output.writeInt(obj.sampleId)
    output.writeString(obj.sampleName)
    output.writeString(obj.contigName)
    output.writeInt(obj.alignmentQuality, true)
    output.writeLong(obj.start, true)
    output.writeString(obj.cigar.toString)
    output.writeBoolean(obj.failedVendorQualityChecks)
    output.writeBoolean(obj.isPositiveStrand)
    output.writeBoolean(obj.isPaired)
  }

  def read(kryo: Kryo, input: Input, klass: Class[MappedRead]): MappedRead = {
    val name = input.readString()
    val count: Int = input.readInt(true)
    val sequenceArray: IndexedSeq[Byte] = input.readBytes(count).toVector
    val qualityScoresArray: IndexedSeq[Byte] = input.readBytes(count).toVector
    val isDuplicate = input.readBoolean()
    val sampleId = input.readInt()
    val sampleName = input.readString().intern()
    val referenceContig = input.readString().intern()
    val alignmentQuality = input.readInt(true)
    val start = input.readLong(true)
    val cigarString = input.readString()
    val failedVendorQualityChecks = input.readBoolean()
    val isPositiveStrand = input.readBoolean()
    val cigar = TextCigarCodec.decode(cigarString)

    val isPaired = input.readBoolean()

    MappedRead(
      name,
      sequenceArray,
      qualityScoresArray,
      isDuplicate,
      sampleId,
      sampleName.intern,
      referenceContig,
      alignmentQuality,
      start,
      cigar,
      failedVendorQualityChecks,
      isPositiveStrand,
      isPaired
    )
  }
}

