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

package org.hammerlab.guacamole.variants

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.adam.util.PhredUtils
import org.hammerlab.guacamole.pileup.Pileup

/**
 *
 * Sample specific pileup and read statistics in support of a given variant
 *
 * @param likelihood probability of the genotype
 * @param readDepth total reads at the genotype position
 * @param alleleReadDepth total reads with allele base at the genotype position
 * @param forwardDepth total reads on the forward strand at the genotype position
 * @param alleleForwardDepth total reads with allele base on the forward strand at the genotype position
 * @param averageMappingQuality average mapping quality of reads
 * @param averageBaseQuality average base quality of bases covering this position
 */
case class AlleleEvidence(likelihood: Double,
                          readDepth: Int,
                          alleleReadDepth: Int,
                          forwardDepth: Int,
                          alleleForwardDepth: Int,
                          averageMappingQuality: Double,
                          averageBaseQuality: Double) {

  lazy val phredScaledLikelihood = PhredUtils.successProbabilityToPhred(likelihood - 1e-10) //subtract small delta to prevent p = 1
  lazy val variantAlleleFrequency = alleleReadDepth.toFloat / readDepth
}

class AlleleEvidenceSerializer extends Serializer[AlleleEvidence] {
  def write(kryo: Kryo, output: Output, obj: AlleleEvidence) = {
    output.writeDouble(obj.likelihood)
    output.writeInt(obj.readDepth)
    output.writeInt(obj.alleleReadDepth)
    output.writeInt(obj.forwardDepth)
    output.writeInt(obj.alleleForwardDepth)

    output.writeDouble(obj.averageMappingQuality)
    output.writeDouble(obj.averageBaseQuality)

  }

  def read(kryo: Kryo, input: Input, klass: Class[AlleleEvidence]): AlleleEvidence = {

    val likelihood = input.readDouble()
    val readDepth = input.readInt()
    val alleleReadDepth = input.readInt()
    val forwardDepth = input.readInt()
    val alleleForwardDepth = input.readInt()

    val averageMappingQuality = input.readDouble()
    val averageBaseQuality = input.readDouble()

    AlleleEvidence(likelihood,
      readDepth,
      alleleReadDepth,
      forwardDepth,
      alleleForwardDepth,
      averageMappingQuality,
      averageBaseQuality
    )

  }
}

trait HasGenotypeEvidenceSerializer {
  lazy val alleleEvidenceSerializer: AlleleEvidenceSerializer = new AlleleEvidenceSerializer
}

object AlleleEvidence {

  def apply(likelihood: Double,
            alleleReadDepth: Int,
            allelePositiveReadDepth: Int,
            pileup: Pileup): AlleleEvidence = {

    AlleleEvidence(
      likelihood,
      pileup.depth,
      alleleReadDepth,
      pileup.positiveDepth,
      allelePositiveReadDepth,
      pileup.elements.map(_.read.alignmentQuality).sum.toFloat / pileup.depth,
      pileup.elements.map(_.qualityScore).sum.toFloat / pileup.depth
    )
  }

  def apply(likelihood: Double, allele: Allele, pileup: Pileup): AlleleEvidence = {
    val (alleleReadDepth, allelePositiveReadDepth) = pileup.alleleReadDepthAndPositiveDepth(allele)
    AlleleEvidence(likelihood, alleleReadDepth, allelePositiveReadDepth, pileup)

  }
}
