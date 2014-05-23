/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.guacamole.somatic

import net.sf.samtools.SAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.LongWritable
import fi.tkk.ics.hadoop.bam.{ AnySAMInputFormat, SAMRecordWritable }
import org.bdgenomics.guacamole.Common

case class SimpleRead(
  baseSequence: Array[Byte],
  referenceContig: String,
  baseQualities: Array[Byte],
  alignmentQuality: Int,
  start: Int,
  end: Int,
  unclippedStart: Int,
  unclippedEnd: Int,
  cigar: net.sf.samtools.Cigar,
  isMapped: Boolean,
  isDuplicate: Boolean)

object SimpleRead {

  /**
   * Convert a SAM tools record into a stripped down SimpleRead case class.
   *
   * @param record
   * @return
   */
  def fromSAM(record: SAMRecord): SimpleRead = {
    val isMapped =
      record.getMappingQuality != SAMRecord.UNKNOWN_MAPPING_QUALITY &&
        record.getReferenceName != null &&
        record.getReferenceIndex >= SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX &&
        record.getAlignmentStart > 0 &&
        record.getUnclippedStart > 0
    val isDuplicate = record.getDuplicateReadFlag
    // SAMRecord start is base 1
    val start = record.getAlignmentStart - 1
    val end = record.getAlignmentEnd - 1
    val unclippedStart = record.getUnclippedStart - 1
    val unclippedEnd = record.getUnclippedEnd - 1
    val cigar = record.getCigar
    SimpleRead(
      baseSequence = record.getReadString.getBytes,
      referenceContig = Reference.normalizeContigName(record.getReferenceName),
      baseQualities = record.getBaseQualities,
      alignmentQuality = record.getMappingQuality,
      start = start,
      end = end,
      unclippedStart = unclippedStart,
      unclippedEnd = unclippedEnd,
      cigar = cigar,
      isMapped = isMapped,
      isDuplicate = isDuplicate)
  }

  /**
   * Given a filename and a spark context, return an RDD of reads represented as samtools records (instead of ADAM).
   *
   * @param filename name of file containing reads
   * @param sc spark context
   * @param mapped if true, will filter out non-mapped reads
   * @param nonDuplicate if true, will filter out duplicate reads.
   * @return
   */
  def loadFile(filename: String,
               sc: SparkContext,
               mapped: Boolean = true,
               nonDuplicate: Boolean = true): RDD[SimpleRead] = {
    val samRecords: RDD[(LongWritable, SAMRecordWritable)] =
      sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, AnySAMInputFormat](filename)
    var reads: RDD[SimpleRead] = samRecords.map({ case (k, v) => fromSAM(v.get) })
    if (mapped) reads = reads.filter(_.isMapped)
    if (nonDuplicate) reads = reads.filter(read => !read.isDuplicate)
    reads.persist()
    val description = (if (mapped) "mapped " else "") + (if (nonDuplicate) "non-duplicate" else "")
    Common.progress(
      "Loaded %,d %s reads into %,d partitions.".format(reads.count, description, reads.partitions.length))
    reads
  }
}