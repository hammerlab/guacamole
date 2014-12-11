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

package org.hammerlab.guacamole.filters

import org.hammerlab.guacamole.pileup.PileupElement
/**
 * Filter to remove pileup elements with low alignment quality
 */
object QualityAlignedReadsFilter {
  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param minimumAlignmentQuality Threshold to define whether a read was poorly aligned
   * @return filtered sequence of elements - those who had higher than minimumAlignmentQuality alignmentQuality
   */
  def apply(elements: Seq[PileupElement], minimumAlignmentQuality: Int): Seq[PileupElement] = {
    elements.filter(_.read.alignmentQuality >= minimumAlignmentQuality)
  }

}

/**
 * Filter to remove pileup elements close to edge of reads
 */
object EdgeBaseFilter {
  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param minimumDistanceFromEndFromRead Threshold of distance from base to edge of read
   * @return filtered sequence of elements - those who were further from directional end minimumDistanceFromEndFromRead
   */
  def apply(elements: Seq[PileupElement], minimumDistanceFromEndFromRead: Int): Seq[PileupElement] = {
    elements.filter(_.distanceFromSequencingEnd >= minimumDistanceFromEndFromRead)
  }
}

