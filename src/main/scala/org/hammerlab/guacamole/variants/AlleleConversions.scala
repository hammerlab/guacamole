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

import org.bdgenomics.formats.avro.{ GenotypeAllele, Genotype => ADAMGenotype }

import scala.collection.JavaConversions

/**
 * Note: ADAM Genotypes really map more to our concept of an "allele", hence some naming dissonance here.
 */
object AlleleConversions {

  def calledAlleleToADAMGenotype(calledAllele: CalledAllele): Seq[ADAMGenotype] = {
    Seq(
      ADAMGenotype.newBuilder
        .setAlleles(JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
        .setSampleId(calledAllele.sampleName)
        .setGenotypeQuality(calledAllele.evidence.phredScaledLikelihood)
        .setReadDepth(calledAllele.evidence.readDepth)
        .setExpectedAlleleDosage(
          calledAllele.evidence.alleleReadDepth.toFloat / calledAllele.evidence.readDepth
        )
        .setReferenceReadDepth(calledAllele.evidence.readDepth - calledAllele.evidence.alleleReadDepth)
        .setAlternateReadDepth(calledAllele.evidence.alleleReadDepth)
        .setVariant(calledAllele.adamVariant)
        .build
    )
  }

  def calledSomaticAlleleToADAMGenotype(calledAllele: CalledSomaticAllele): Seq[ADAMGenotype] = {
    Seq(
      ADAMGenotype.newBuilder
        .setAlleles(JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
        .setSampleId(calledAllele.sampleName)
        .setGenotypeQuality(calledAllele.phredScaledSomaticLikelihood)
        .setReadDepth(calledAllele.tumorEvidence.readDepth)
        .setExpectedAlleleDosage(
          calledAllele.tumorEvidence.alleleReadDepth.toFloat / calledAllele.tumorEvidence.readDepth
        )
        .setReferenceReadDepth(calledAllele.tumorEvidence.readDepth - calledAllele.tumorEvidence.alleleReadDepth)
        .setAlternateReadDepth(calledAllele.tumorEvidence.alleleReadDepth)
        .setVariant(calledAllele.adamVariant)
        .build
    )
  }
}

