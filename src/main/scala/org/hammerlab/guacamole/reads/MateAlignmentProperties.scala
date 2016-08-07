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

package org.hammerlab.guacamole.reads

import htsjdk.samtools.SAMRecord
import org.hammerlab.guacamole.reference.{ContigName, Locus}

/**
 * Details of the mate read alignment
 * @param contigName Contig/chromosome of the mate read
 * @param start 0-based start position of the mate read
 * @param inferredInsertSize Insert size between the reads if defined
 * @param isPositiveStrand Whether the mate is on the positive strand
 */
case class MateAlignmentProperties(contigName: ContigName,
                                   start: Locus,
                                   inferredInsertSize: Option[Int],
                                   isPositiveStrand: Boolean) {
}

object MateAlignmentProperties {
  def apply(record: SAMRecord): Option[MateAlignmentProperties] = {
    if (!record.getMateUnmappedFlag)
      Some(
        MateAlignmentProperties(
          contigName = record.getMateReferenceName,
          start = record.getMateAlignmentStart - 1, //subtract 1 from start, since samtools is 1-based and we're 0-based.
          inferredInsertSize =
            if (record.getInferredInsertSize != 0)
              Some(record.getInferredInsertSize)
            else
              None,
          isPositiveStrand = !record.getMateNegativeStrandFlag
        )
      )
    else
      None
  }
}
