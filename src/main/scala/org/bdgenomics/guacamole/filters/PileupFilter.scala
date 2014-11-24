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

package org.bdgenomics.guacamole.filters

import org.bdgenomics.guacamole.Common.Arguments.Base
import org.bdgenomics.guacamole.pileup.{ Pileup, PileupElement }
import org.kohsuke.args4j.Option

/**
 * This is a cheap computation for a region's complexity
 * We define a region's complexity as the number of reads whose alignment quality was below a certain threshold
 * (minimumAlignmentQuality) or reads have unmapped or incorrectly mapped pairs
 *
 * This filter eliminates pileups where the number of low quality aligned reads is above the maximumAlignmentComplexity
 * or reads have unmapped or incorrectly mapped pairs
 */
object RegionComplexityFilter {

  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param maximumAlignmentComplexity The maximum allowed percent of reads to be poorly aligned (as defined by minimumAlignmentQuality)
   * @param minimumAlignmentQuality Threshold to define whether a read was poorly aligned
   * @return Empty sequence if the number of poorly mapped reads surpass the maximumAlignmentComplexity
   */
  def apply(elements: Seq[PileupElement], maximumAlignmentComplexity: Int, minimumAlignmentQuality: Int): Seq[PileupElement] = {
    val depth = elements.length
    val qualityMappedReads = elements.filter(el =>
      el.read.alignmentQuality >= minimumAlignmentQuality &&
        (for {
          mp <- el.read.matePropertiesOpt
          if mp.isMateMapped
          mateReferenceContig <- mp.mateReferenceContig
          if mateReferenceContig == el.read.referenceContig
        } yield {
          Some(true)
        }).isDefined
    ).length

    if (math.ceil((depth - qualityMappedReads) * 100.0 / depth) < maximumAlignmentComplexity) {
      elements
    } else {
      Seq.empty
    }
  }
}

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

    @Option(name = "--max-mapping-complexity", usage = "Maximum percent of reads that can be mapped with low quality (indicative of a complex region")
    var maxMappingComplexity: Int = 20

    @Option(name = "--min-alignment-for-complexity", usage = "Minimum read mapping quality for a read (Phred-scaled) that counts towards poorly mapped for complexity (default: 1)")
    var minAlignmentForComplexity: Int = 1

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
      args.maxMappingComplexity,
      args.minAlignmentForComplexity,
      args.minAlignmentQuality,
      args.maxPercentAbnormalInsertSize,
      args.minEdgeDistance)

  }

  def apply(pileup: Pileup,
            filterMultiAllelic: Boolean,
            maxMappingComplexity: Int,
            minAlignmentForComplexity: Int,
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

    if (maxMappingComplexity < 100) {
      elements = RegionComplexityFilter(elements, maxMappingComplexity, minAlignmentForComplexity)
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
