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

import org.bdgenomics.formats.avro.{Contig, DatabaseVariantAnnotation, Variant}
import org.hammerlab.guacamole.reference.{ContigName, ReferenceRegion}
import org.hammerlab.guacamole.util.Bases

/**
 * Base properties of a genomic change in a sequence sample from a reference genome
 */
trait ReferenceVariant extends ReferenceRegion {

  def sampleName: String

  /** reference and sequenced bases for this variant */
  def allele: Allele

  /** Conversion to ADAMVariant */
  def adamVariant = Variant.newBuilder
    .setStart(start)
    .setEnd(end)
    .setReferenceAllele(Bases.basesToString(allele.refBases))
    .setAlternateAllele(Bases.basesToString(allele.altBases))
    .setContig(Contig.newBuilder.setContigName(contigName).build)
    .build

  def rsID: Option[Int]

  def adamVariantDatabase = {
    val builder = DatabaseVariantAnnotation.newBuilder
    rsID.foreach(builder.setDbSnpId(_))
    builder.build
  }
}
