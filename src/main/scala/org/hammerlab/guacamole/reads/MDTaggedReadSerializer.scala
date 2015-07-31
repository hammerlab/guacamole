package org.hammerlab.guacamole.reads

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import htsjdk.samtools.TextCigarCodec

/**
 * Created by danvk on 7/31/15.
 */
class MDTaggedReadSerializer extends Serializer[MDTaggedRead] {
  def write(kryo: Kryo, output: Output, obj: MDTaggedRead) = {
    new MappedReadSerializer().write(kryo, output, obj)
  }

  def read(kryo: Kryo, input: Input, klass: Class[MDTaggedRead]): MDTaggedRead = {
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
    val cigar = TextCigarCodec.decode(cigarString)

    val isPaired = input.readBoolean()

    new MDTaggedRead(
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
      isPaired
    )
  }
}
