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

package org.hammerlab.guacamole.filters.genotype

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.variants.{AlleleEvidence, CalledAllele}

/**
 * Filter to remove genotypes where the number of reads at the locus is too low or too high
 */
object ReadDepthFilter {

  def withinReadDepthRange(alleleEvidence: AlleleEvidence,
                           minReadDepth: Int,
                           maxReadDepth: Int): Boolean = {
    alleleEvidence.readDepth >= minReadDepth && alleleEvidence.readDepth < maxReadDepth
  }

  /**
   *
   *  Apply the filter to an RDD of genotypes
   *
   * @param genotypes RDD of genotypes to filter
   * @param minReadDepth minimum number of reads at locus for this genotype
   * @param maxReadDepth maximum number of reads at locus for this genotype
   * @param debug if true, compute the count of genotypes after filtering
   * @return Genotypes with read depth >= minReadDepth and < maxReadDepth
   */
  def apply(genotypes: RDD[CalledAllele],
            minReadDepth: Int,
            maxReadDepth: Int,
            debug: Boolean = false): RDD[CalledAllele] = {
    val filteredGenotypes = genotypes.filter(gt => withinReadDepthRange(gt.evidence, minReadDepth, maxReadDepth))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}








