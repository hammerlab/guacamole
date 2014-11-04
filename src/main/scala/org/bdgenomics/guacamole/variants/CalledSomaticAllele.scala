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
import org.bdgenomics.adam.util.PhredUtils

/**
 *
 * A variant that exists in a tumor sample, but not in the normal sample; includes supporting read statistics from both samples
 *
 * @param sampleName sample the variant was called on
 * @param referenceContig chromosome or genome contig of the variant
 * @param start start position of the variant (0-based)
 * @param allele reference and sequence bases of this variant
 * @param somaticLogOdds log odds-ratio of the variant in the tumor compared to the normal sample
 * @param tumorEvidence supporting statistics for the variant in the tumor sample
 * @param normalEvidence supporting statistics for the variant in the normal sample
 * @param length length of the variant
 */
case class CalledSomaticAllele(sampleName: String,
                               referenceContig: String,
                               start: Long,
                               allele: Allele,
                               somaticLogOdds: Double,
                               tumorEvidence: AlleleEvidence,
                               normalEvidence: AlleleEvidence,
                               length: Int = 1) extends ReferenceVariant {
  val end: Long = start + 1L

  // P ( variant in tumor AND no variant in normal) = P(variant in tumor) * ( 1 - P(variant in normal) )
  lazy val phredScaledSomaticLikelihood =
    PhredUtils.successProbabilityToPhred(tumorEvidence.likelihood * (1 - normalEvidence.likelihood) - 1e-10)
}

class CalledSomaticAlleleSerializer
    extends Serializer[CalledSomaticAllele]
    with HasGenotypeEvidenceSerializer
    with HasAlleleSerializer {

  def write(kryo: Kryo, output: Output, obj: CalledSomaticAllele) = {
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeLong(obj.start)
    alleleSerializer.write(kryo, output, obj.allele)
    output.writeDouble(obj.somaticLogOdds)

    alleleEvidenceSerializer.write(kryo, output, obj.tumorEvidence)
    alleleEvidenceSerializer.write(kryo, output, obj.normalEvidence)

    output.writeInt(obj.length, true)

  }

  def read(kryo: Kryo, input: Input, klass: Class[CalledSomaticAllele]): CalledSomaticAllele = {

    val sampleName: String = input.readString()
    val referenceContig: String = input.readString()
    val start: Long = input.readLong()
    val allele: Allele = alleleSerializer.read(kryo, input, classOf[Allele])
    val somaticLogOdds = input.readDouble()

    val tumorEvidence = alleleEvidenceSerializer.read(kryo, input, classOf[AlleleEvidence])
    val normalEvidence = alleleEvidenceSerializer.read(kryo, input, classOf[AlleleEvidence])

    val length: Int = input.readInt(true)

    CalledSomaticAllele(
      sampleName,
      referenceContig,
      start,
      allele,
      somaticLogOdds,
      tumorEvidence = tumorEvidence,
      normalEvidence = normalEvidence
    )

  }

}
