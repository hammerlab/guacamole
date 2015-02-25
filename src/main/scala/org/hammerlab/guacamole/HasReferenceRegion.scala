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

package org.hammerlab.guacamole

/**
 * Trait for objects that are associated with an interval on the genome. The most prominent example is a
 * [[org.hammerlab.guacamole.reads.MappedRead]], but there's also [[org.hammerlab.guacamole.variants.ReferenceVariant]].
 *
 * @todo replace with ReferenceRegion base class in ADAM
 */
trait HasReferenceRegion {

  /** Name of the reference contig */
  val referenceContig: String

  /** Start position on the genome, inclusive. Must be non-negative. */
  val start: Long

  /** The end position on the genome, *exclusive*. Must be non-negative. */
  val end: Long

  /**
   * Does this region overlap any of the given loci, with halfWindowSize padding?
   *
   * The padding is inclusive on both ends, so for example if halfWindowSize=1, then a region would overlap if it
   * included any of these three positions: locus - 1, locus, and locus + 1.
   */
  def overlapsLociSet(loci: LociSet, halfWindowSize: Long = 0): Boolean = {
    loci.onContig(referenceContig).intersects(math.max(0, start - halfWindowSize), end + halfWindowSize)
  }

  /**
   * Does the region overlap the given locus, with halfWindowSize padding?
   */
  def overlapsLocus(locus: Long, halfWindowSize: Long = 0): Boolean = {
    start - halfWindowSize <= locus && end + halfWindowSize > locus
  }

  /**
   * Does the region overlap another reference region
   * @param other another region on the genome
   * @return True if the the regions overlap
   */
  def overlaps(other: HasReferenceRegion): Boolean = {
    other.referenceContig == referenceContig && (overlapsLocus(other.start) || other.overlapsLocus(start))
  }
}

