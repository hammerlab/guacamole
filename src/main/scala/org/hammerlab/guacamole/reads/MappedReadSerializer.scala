/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    output.writeString(obj.sampleName)
    output.writeString(obj.contig)
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

