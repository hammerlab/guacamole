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

package org.bdgenomics.guacamole.variants

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }

/**
 *
 * A variant that exists in the sample; includes supporting read statistics
 *
 * @param sampleName sample the variant was called on
 * @param referenceContig chromosome or genome contig of the variant
 * @param start start position of the variant (0-based)
 * @param allele allele (ref + seq bases) for this variant
 * @param evidence supporting statistics for the variant
 * @param length length of the variant
 */
case class CalledAllele(sampleName: String,
                        referenceContig: String,
                        start: Long,
                        allele: Allele,
                        evidence: AlleleEvidence,
                        length: Int = 1) extends ReferenceVariant {
  val end: Long = start + 1L

}

class CalledAlleleSerializer
    extends Serializer[CalledAllele]
    with HasAlleleSerializer
    with HasGenotypeEvidenceSerializer {

  def write(kryo: Kryo, output: Output, obj: CalledAllele) = {
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeLong(obj.start, true)
    alleleSerializer.write(kryo, output, obj.allele)
    alleleEvidenceSerializer.write(kryo, output, obj.evidence)
    output.writeInt(obj.length, true)
  }

  def read(kryo: Kryo, input: Input, klass: Class[CalledAllele]): CalledAllele = {

    val sampleName: String = input.readString()
    val referenceContig: String = input.readString()
    val start: Long = input.readLong(true)
    val allele = alleleSerializer.read(kryo, input, classOf[Allele])
    val evidence = alleleEvidenceSerializer.read(kryo, input, classOf[AlleleEvidence])
    val length: Int = input.readInt(true)

    CalledAllele(
      sampleName,
      referenceContig,
      start,
      allele,
      evidence,
      length
    )

  }
}

