package org.hammerlab.guacamole.reads

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }

// Serialization: UnmappedRead
class UnmappedReadSerializer extends Serializer[UnmappedRead] {
  def write(kryo: Kryo, output: Output, obj: UnmappedRead) = {
    output.writeString(obj.name)
    assert(obj.sequence.length == obj.baseQualities.length)
    output.writeInt(obj.sequence.length, true)
    output.writeBytes(obj.sequence.toArray)
    output.writeBytes(obj.baseQualities.toArray)
    output.writeBoolean(obj.isDuplicate)
    output.writeInt(obj.sampleId)
    output.writeString(obj.sampleName)
    output.writeBoolean(obj.failedVendorQualityChecks)
    output.writeBoolean(obj.isPaired)

  }

  def read(kryo: Kryo, input: Input, klass: Class[UnmappedRead]): UnmappedRead = {
    val name: String = input.readString()
    val count: Int = input.readInt(true)
    val sequenceArray: Vector[Byte] = input.readBytes(count).toVector
    val qualityScoresArray = input.readBytes(count).toVector
    val isDuplicate = input.readBoolean()
    val sampleId = input.readInt()
    val sampleName = input.readString().intern()
    val failedVendorQualityChecks = input.readBoolean()
    val isPaired = input.readBoolean()

    UnmappedRead(
      name,
      sequenceArray,
      qualityScoresArray,
      isDuplicate,
      sampleId,
      sampleName.intern,
      failedVendorQualityChecks,
      isPaired
    )
  }
}

