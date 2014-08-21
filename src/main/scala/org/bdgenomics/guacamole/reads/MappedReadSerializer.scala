package org.bdgenomics.guacamole.reads

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import net.sf.samtools.TextCigarCodec

// Serialization: MappedRead
class MappedReadSerializer extends Serializer[MappedRead] {
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
    output.writeBoolean(obj.isPaired)
    output.writeBoolean(obj.isFirstInPair)
    obj.inferredInsertSize match {
      case None =>
        output.writeBoolean(false)
      case Some(insertSize) =>
        output.writeBoolean(true)
        output.writeInt(insertSize)
    }
    if (obj.isMateMapped) {
      output.writeBoolean(true)
      output.writeString(obj.mateReferenceContig.get)
      output.writeLong(obj.mateStart.get)
    } else {
      output.writeBoolean(false)
    }
    output.writeBoolean(obj.isMatePositiveStrand)
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
    val isPairedRead = input.readBoolean()
    val isFirstInPair = input.readBoolean()
    val hasInferredInsertSize = input.readBoolean()
    val inferredInsertSize = if (hasInferredInsertSize) Some(input.readInt()) else None
    val isMateMapped = input.readBoolean()
    val mateReferenceContig = if (isMateMapped) Some(input.readString()) else None
    val mateStart = if (isMateMapped) Some(input.readLong()) else None
    val isMatePositiveStrand = input.readBoolean()

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
      isPairedRead,
      isFirstInPair,
      inferredInsertSize,
      isMateMapped,
      mateReferenceContig,
      mateStart,
      isMatePositiveStrand)
  }
}

