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

package org.bdgenomics.guacamole

import scala.collection.mutable
import org.apache.spark.Logging

/**
 * Suppose we have a set of loci on a given contig and some objects that are mapped to that contig, and at each locus we
 * want to look at the regions that overlap a window of a certain number of bases surrounding that locus. This class
 * implements this "sliding window" functionality.
 *
 * After instantiating this class, call [[SlidingWindow.setCurrentLocus( )]] repeatedly for each
 * locus being considered. After calling this method, the [[SlidingWindow.currentRegions]] property will contain the
 * objects that overlap the current locus.
 *
 * To enable an efficient implementation, we require that both the sequence of loci to be considered and the iterator
 * of objects are sorted.
 *
 * @param halfWindowSize Number of nucleotide bases to either side of the specified locus to provide regions for. For
 *                       example, if halfWindowSize=5, and our currentLocus=100, then currentRegions will include regions
 *                       that map to anywhere between 95 and 105, inclusive. Set to 0 to consider only regions that
 *                       overlap the exact locus being considered, with no surrounding window.
 *
 * @param rawSortedRegions Iterator of regions, sorted by the aligned start locus.
 */
case class SlidingWindow[Region <: ReferenceRegion](halfWindowSize: Long, rawSortedRegions: Iterator[Region]) extends Logging {
  /** The locus currently under consideration. */
  var currentLocus = -1L

  /** The new regions that were added to currentRegions as a result of the most recent call to setCurrentLocus. */
  var newRegions: Seq[Region] = Seq.empty

  private var referenceName: Option[String] = None
  private var mostRecentRegionStart: Long = 0
  private val sortedRegions: BufferedIterator[Region] = rawSortedRegions.map(region => {
    if (referenceName.isEmpty) referenceName = Some(region.referenceContig)
    require(region.referenceContig == referenceName.get, "Regions must have the same reference name")
    require(region.start >= mostRecentRegionStart, "Regions must be sorted by start locus")
    mostRecentRegionStart = region.start

    region
  }).buffered

  private val currentRegionsPriorityQueue = {
    // Order regions by end locus, increasing.
    def regionOrdering = new Ordering[Region] {
      def compare(first: Region, second: Region) = second.end.compare(first.end)
    }
    new mutable.PriorityQueue[Region]()(regionOrdering)
  }

  /** The regions that overlap the window surrounding [[currentLocus]]. */
  def currentRegions(): Seq[Region] = {
    currentRegionsPriorityQueue.toSeq
  }

  /**
   * Advance to the specified locus, which must be greater than the current locus. After calling this, the
   * [[currentRegions]] method will give the overlapping regions at the new locus.
   *
   * @param locus Locus to advance to.
   * @return The *new regions* that were added as a result of this call. Note that this is not the full set of regions in
   *         the window: you must examine [[currentRegions]] for that.
   */
  def setCurrentLocus(locus: Long): Seq[Region] = {
    assume(locus >= currentLocus, "Pileup window can only move forward in locus")
    currentLocus = locus

    def overlaps(region: Region) = {
      region.start <= locus + halfWindowSize && (region.end - 1) >= locus - halfWindowSize
    }

    // Remove regions that are no longer in the window.
    while (!currentRegionsPriorityQueue.isEmpty && (currentRegionsPriorityQueue.head.end - 1) < locus - halfWindowSize) {
      val dropped = currentRegionsPriorityQueue.dequeue()
      assert(!overlaps(dropped))
    }

    newRegions = if (sortedRegions.isEmpty) {
      Seq.empty
    } else {
      // Build up a list of new regions that are now in the window.
      val newRegionsBuilder = mutable.ArrayBuffer.newBuilder[Region]
      while (sortedRegions.nonEmpty && sortedRegions.head.start <= locus + halfWindowSize) {
        val region = sortedRegions.next()
        if (overlaps(region)) newRegionsBuilder += region
      }
      newRegionsBuilder.result
    }
    currentRegionsPriorityQueue.enqueue(newRegions: _*)
    //newRegions(currentRegionsPriorityQueue.forall(overlaps)) // Correctness check.
    newRegions // We return the newly added regions.
  }

  /**
   * The start locus of the next region in the (sorted) iterator.
   */
  def nextStartLocus(): Option[Long] = {
    if (sortedRegions.hasNext) Some(sortedRegions.head.start) else None
  }
}
