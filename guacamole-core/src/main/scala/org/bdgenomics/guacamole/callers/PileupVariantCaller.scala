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

package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.{ Pileup, LociSet, SlidingReadWindow }
import org.bdgenomics.adam.avro.ADAMGenotype

/**
 * A [[PileupVariantCaller]] is a [[SlidingWindowVariantCaller]] that examines only a single pileup at each locus.
 *
 */
trait PileupVariantCaller extends SlidingWindowVariantCaller {

  // We consider each locus independently of all others.
  override val halfWindowSize = 0L

  override def callVariants(samples: Seq[String], reads: SlidingReadWindow, loci: LociSet.SingleContig): Iterator[ADAMGenotype] = {
    val lociAndReads = loci.individually.map(locus => (locus, reads.setCurrentLocus(locus)))
    val pileupsIterator = Pileup.pileupsAtLoci(lociAndReads)
    pileupsIterator.flatMap(callVariantsAtLocus _)
  }

  /**
   * Implementations must override this.
   * @param pileup The Pileup at a particular locus.
   * @return ADAMGenotype instances for the called variants, if any.
   */
  def callVariantsAtLocus(pileup: Pileup): Seq[ADAMGenotype]
}
