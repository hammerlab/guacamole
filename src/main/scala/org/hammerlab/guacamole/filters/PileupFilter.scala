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

import org.hammerlab.guacamole.Common.Arguments.Base
import org.hammerlab.guacamole.pileup.{ Pileup, PileupElement }
import org.kohsuke.args4j.Option

/**
 * Filter to remove pileups which may produce multi-allelic variant calls.
 * These are generally more complex and more difficult to call
 */
object MultiAllelicPileupFilter {

  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param maxPloidy number of alleles to expect (> maxPloidy would mean multiple possible alternates) default: 2
   * @return Empty sequence if there are > maxPloidy possible allelee, otherwise original set of elements
   */
  def apply(elements: Seq[PileupElement], maxPloidy: Int = 2): Seq[PileupElement] = {
    if (elements.map(_.allele).distinct.length > maxPloidy) {
      Seq.empty
    } else {
      elements
    }
  }
}

object PileupFilter {

  trait PileupFilterArguments extends Base {

    @Option(name = "--min-mapq", usage = "Minimum read mapping quality for a read (Phred-scaled). (default: 1)")
    var minAlignmentQuality: Int = 1
    
    @Option(name = "--filter-multi-allelic", usage = "Filter any pileups > 2 bases considered")
    var filterMultiAllelic: Boolean = false

    @Option(name = "--min-edge-distance", usage = "Filter reads where the base in the pileup is closer than minEdgeDistance to the (directional) end of the read")
    var minEdgeDistance: Int = 0

  }

  def apply(pileup: Pileup, args: PileupFilterArguments): Pileup = {
    apply(pileup,
      args.filterMultiAllelic,
      args.minAlignmentQuality,
      args.minEdgeDistance)

  }

  def apply(pileup: Pileup,
            filterMultiAllelic: Boolean,
            minAlignmentQuality: Int,
            minEdgeDistance: Int): Pileup = {

    var elements: Seq[PileupElement] = pileup.elements

    if (filterMultiAllelic) {
      elements = MultiAllelicPileupFilter(elements)
    }

    if (minAlignmentQuality > 0) {
      elements = QualityAlignedReadsFilter(elements, minAlignmentQuality)
    }

    if (minEdgeDistance > 0) {
      elements = EdgeBaseFilter(elements, minEdgeDistance)
    }

    Pileup(pileup.locus, pileup.referenceBase, elements)
  }
}
