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

import htsjdk.samtools.CigarElement

object CigarUtils {
  /**
   * The length of a cigar element in read coordinate space.
   *
   * @param element Cigar Element
   * @return Length of CigarElement, 0 if it does not consume any read bases.
   */
  def getReadLength(element: CigarElement): Int = {
    if (element.getOperator.consumesReadBases) element.getLength else 0
  }

  /**
   * The length of a cigar element in reference coordinate space.
   *
   * @param element Cigar Element
   * @return Length of CigarElement, 0 if it does not consume any reference bases.
   */
  def getReferenceLength(element: CigarElement): Int = {
    if (element.getOperator.consumesReferenceBases) element.getLength else 0
  }
}
