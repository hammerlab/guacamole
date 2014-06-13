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
 * Suppose we have a set of loci on a given contig and some reads that are mapped to that contig, and at each locus we
 * want to look at the reads that overlap a window of a certain number of bases surrounding that locus. This class
 * implements this "sliding window" functionality.
 *
 * After instantiating this class, call [[SlidingReadWindow.setCurrentLocus( )]] repeatedly for each
 * locus being considered. After calling this method, the [[SlidingReadWindow.currentReads]] property will contain the
 * reads that overlap the current locus.
 *
 * To enable an efficient implementation, we require that both the sequence of loci to be considered and the iterator
 * of reads are sorted.
 *
 * @param halfWindowSize Number of nucleotide bases to either side of the specified locus to provide reads for. For
 *                       example, if halfWindowSize=5, and our currentLocus=100, then currentReads will include reads
 *                       that map to anywhere between 95 and 105, inclusive. Set to 0 to consider only reads that
 *                       overlap the exact locus being considered, with no surrounding window.
 *
 * @param rawSortedReads Iterator of aligned reads, sorted by the aligned start locus.
 */
case class SlidingReadWindow(halfWindowSize: Long, rawSortedReads: Iterator[MappedRead]) extends Logging {
  /** The locus currently under consideration. */
  var currentLocus = -1L

  var referenceName: Option[String] = None
  private var mostRecentReadStart: Long = 0
  private val sortedReads: BufferedIterator[MappedRead] = rawSortedReads.map(read => {
    if (referenceName.isEmpty) referenceName = Some(read.referenceContig)
    require(read.referenceContig == referenceName.get, "Reads must have the same reference name")
    require(read.start >= mostRecentReadStart, "Reads must be sorted by start locus")
    mostRecentReadStart = read.start
    require(read.cigar != null, "Reads must have a CIGAR")
    read
  }).buffered

  private val currentReadsPriorityQueue = {
    // Order reads by end locus, increasing.
    def readOrdering = new Ordering[MappedRead] {
      def compare(first: MappedRead, second: MappedRead) = second.end.compare(first.end)
    }
    new mutable.PriorityQueue[MappedRead]()(readOrdering)
  }

  /** The reads that overlap the window surrounding [[currentLocus]]. */
  def currentReads(): Seq[MappedRead] = {
    currentReadsPriorityQueue.toSeq
  }

  /**
   * Advance to the specified locus, which must be greater than the current locus. After calling this, the
   * [[currentReads]] method will give the overlapping reads at the new locus.
   *
   * @param locus Locus to advance to.
   * @return The *new reads* that were added as a result of this call. Note that this is not the full set of reads in
   *         the window: you must examine [[currentReads]] for that.
   */
  def setCurrentLocus(locus: Long): Seq[MappedRead] = {
    assume(locus >= currentLocus, "Pileup window can only move forward in locus")
    currentLocus = locus

    def overlaps(read: MappedRead) = {
      read.start <= locus + halfWindowSize && (read.end - 1) >= locus - halfWindowSize
    }

    // Remove reads that are no longer in the window.
    while (!currentReadsPriorityQueue.isEmpty && (currentReadsPriorityQueue.head.end - 1) < locus - halfWindowSize) {
      val dropped = currentReadsPriorityQueue.dequeue()
      assert(!overlaps(dropped))
    }

    if (sortedReads.isEmpty) {
      Seq.empty
    } else {
      // Build up a list of new reads that are now in the window.
      val newReadsBuilder = mutable.ArrayBuffer.newBuilder[MappedRead]
      while (sortedReads.nonEmpty && sortedReads.head.start <= locus + halfWindowSize) {
        val read = sortedReads.next()
        if (overlaps(read)) newReadsBuilder += read
      }
      val newReads = newReadsBuilder.result

      currentReadsPriorityQueue.enqueue(newReads: _*)
      assert(currentReadsPriorityQueue.forall(overlaps)) // Correctness check.
      newReads // We return the newly added reads.
    }
  }

  def nextStartLocus(): Option[Long] = {
    if (sortedReads.hasNext)
      Some(sortedReads.head.start)
    else
      None
  }
}
