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

/**
 * Filter to remove pileups where there are many reads with abnormal insert size
 */
object AbnormalInsertSizePileupFilter {

  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param maxAbnormalInsertSizeReadsThreshold maximum allowed percent of reads that have an abnormal insert size
   * @param minInsertSize smallest insert size considered normal (default: 5)
   * @param maxInsertSize largest insert size considered normal (default: 1000)
   * @return Empty sequence if there are more than maxAbnormalInsertSizeReadsThreshold % reads with insert size out of the specified range
   */
  def apply(elements: Seq[PileupElement],
            maxAbnormalInsertSizeReadsThreshold: Int,
            minInsertSize: Int = 5, maxInsertSize: Int = 1000): Seq[PileupElement] = {
    val abnormalInsertSizeReads = elements.count(
      _.read.matePropertiesOpt
        .flatMap(_.inferredInsertSize)
        .exists(inferredInsertSize =>
          math.abs(inferredInsertSize) < minInsertSize ||
            math.abs(inferredInsertSize) > maxInsertSize
        )
    )
    if (100.0 * abnormalInsertSizeReads / elements.length > maxAbnormalInsertSizeReadsThreshold) {
      Seq.empty
    } else {
      elements
    }
  }
}

object DeletionEvidencePileupFilter {
  /**
   *
   * @param elements sequence of pileup elements to filter
   * @return Empty sequence if they overlap deletion
   */
  def apply(elements: Seq[PileupElement]): Seq[PileupElement] = {
    if (elements.exists(_.isDeletion)) {
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

    @Option(name = "--max-percent-abnormal-insert-size", usage = "Filter pileups where % of reads with abnormal insert size is greater than specified (default: 100)")
    var maxPercentAbnormalInsertSize: Int = 100

    @Option(name = "--filter-multi-allelic", usage = "Filter any pileups > 2 bases considered")
    var filterMultiAllelic: Boolean = false

    @Option(name = "--min-edge-distance", usage = "Filter reads where the base in the pileup is closer than minEdgeDistance to the (directional) end of the read")
    var minEdgeDistance: Int = 0

  }

  def apply(pileup: Pileup, args: PileupFilterArguments): Pileup = {
    apply(pileup,
      args.filterMultiAllelic,
      args.minAlignmentQuality,
      args.maxPercentAbnormalInsertSize,
      args.minEdgeDistance)

  }

  def apply(pileup: Pileup,
            filterMultiAllelic: Boolean,
            minAlignmentQuality: Int,
            maxPercentAbnormalInsertSize: Int,
            minEdgeDistance: Int): Pileup = {

    var elements: Seq[PileupElement] = pileup.elements

    if (filterMultiAllelic) {
      elements = MultiAllelicPileupFilter(elements)
    }

    if (maxPercentAbnormalInsertSize < 100) {
      elements = AbnormalInsertSizePileupFilter(elements, maxPercentAbnormalInsertSize)
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
