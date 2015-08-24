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

import htsjdk.samtools.{ SAMRecord, Cigar }
import org.bdgenomics.adam.util.{ PhredUtils, MdTag }
import org.hammerlab.guacamole.{ Bases, HasReferenceRegion }

import scala.collection.JavaConversions

/**
 * A mapped read. See the [[Read]] trait for some of the field descriptions.
 *
 * @param referenceContig the contig name (e.g. "chr12") that this read was mapped to.
 * @param alignmentQuality the mapping quality, phred scaled.
 * @param start the (0-based) reference locus that the first base in this read aligns to.
 * @param cigar parsed samtools CIGAR object.
 */
case class MappedRead(
    token: Int,
    sequence: Seq[Byte],
    baseQualities: Seq[Byte],
    isDuplicate: Boolean,
    sampleName: String,
    referenceContig: String,
    alignmentQuality: Int,
    start: Long,
    cigar: Cigar,
    mdTagOpt: Option[MdTag],
    failedVendorQualityChecks: Boolean,
    isPositiveStrand: Boolean,
    isPaired: Boolean) extends Read with HasReferenceRegion {

  assert(baseQualities.length == sequence.length,
    "Base qualities have length %d but sequence has length %d".format(baseQualities.length, sequence.length))

  override val isMapped = true
  override val hasMdTag = mdTagOpt.isDefined

  lazy val referenceBases: Seq[Byte] =
    mdTagOpt match {
      case None => throw new ReferenceWithoutMDTagException(this)
      case Some(mdTag) => try {
        MDTagUtils.getReference(mdTag, sequence, cigar, allowNBase = true)
      } catch {
        case e: IllegalStateException => throw new CigarMDTagMismatchException(cigar, mdTag, e)
      }
    }

  /**
   * Find the reference base at a given locus for any locus that overlaps this read sequence
   *
   * @param referenceLocus Locus (0-based) at which to look up the reference base
   * @return  The reference base at the given locus
   */
  def getReferenceBaseAtLocus(referenceLocus: Long): Byte = {
    assume(referenceLocus >= start && referenceLocus < end)
    referenceBases((referenceLocus - start).toInt)
  }

  lazy val alignmentLikelihood = PhredUtils.phredToSuccessProbability(alignmentQuality)

  /** Individual components of the CIGAR string (e.g. "10M"), parsed, and as a Scala buffer. */
  val cigarElements = JavaConversions.asScalaBuffer(cigar.getCigarElements)

  /**
   * The end of the alignment, exclusive. This is the first reference locus AFTER the locus corresponding to the last
   * base in this read.
   */
  val end: Long = start + cigar.getPaddedReferenceLength

  /**
   * A read can be "clipped", meaning that some prefix or suffix of it did not align. This is the start of the whole
   * read's alignment, including any initial clipped bases.
   */
  val unclippedStart = cigarElements.takeWhile(Read.cigarElementIsClipped).foldLeft(start)({
    (pos, element) => pos - element.getLength
  })

  /**
   * The end of the read's alignment, including any final clipped bases, exclusive.
   */
  val unclippedEnd = cigarElements.reverse.takeWhile(Read.cigarElementIsClipped).foldLeft(end)({
    (pos, element) => pos + element.getLength
  })

  override def toString(): String =
    "MappedRead(%s:%d, %s, %s, %s)".format(
      referenceContig, start,
      cigar.toString,
      mdTagOpt.map(_.toString),
      Bases.basesToString(sequence)
    )
}

object MappedRead {
  def apply(
    token: Int,
    sequence: Seq[Byte],
    baseQualities: Seq[Byte],
    isDuplicate: Boolean,
    sampleName: String,
    referenceContig: String,
    alignmentQuality: Int,
    start: Long,
    cigar: Cigar,
    mdTagString: Option[String],
    failedVendorQualityChecks: Boolean,
    isPositiveStrand: Boolean,
    isPaired: Boolean)(implicit d: DummyImplicit): MappedRead = MappedRead(
    token, sequence, baseQualities, isDuplicate, sampleName, referenceContig,
    alignmentQuality, start, cigar,
    mdTagString.map(MdTag(_, start)),
    failedVendorQualityChecks, isPositiveStrand, isPaired)
}

case class MissingMDTagException(record: SAMRecord)
  extends Exception(s"Missing MDTag in SAMRecord: $record")

case class CigarMDTagMismatchException(cigar: Cigar, mdTag: MdTag, cause: IllegalStateException)
  extends Exception(s"Cigar $cigar seems inconsistent with MD tag $mdTag", cause)

case class ReferenceWithoutMDTagException(read: MappedRead)
  extends Exception(s"Attempted to get reference data for a read without an MD tag: $read")
