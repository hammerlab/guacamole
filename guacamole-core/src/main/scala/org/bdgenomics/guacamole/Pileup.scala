package org.bdgenomics.guacamole

import org.bdgenomics.adam.rich.DecadentRead
import net.sf.samtools.{CigarElement, Cigar, CigarOperator, TextCigarCodec}
import org.bdgenomics.adam.util.MdTag
import scala.collection.parallel.mutable

case class Pileup(elements: Seq[PileupElement]) {
  lazy val head = elements.head
  lazy val locus: Long = head.locus
  lazy val referenceName: String = head.read.record.referenceName

  assume(elements.forall(_.read.record.referenceName == referenceName), "Reads in pileup have mismatching reference names")
  assume(elements.forall(_.locus == locus), "Reads in pileup have mismatching loci")

  lazy val referenceBase: String = {
    head.read.record.mdTag.get.getReference(head.read.record).charAt(head.locus - head.read.record.start)
  }

  lazy val bySample: Map[String, Pileup] = {
    elements.groupBy(_.read.record.recordGroupSample.toString).map({case (sample, elements) => (sample, Pileup(elements))})
  }

  def atGreaterLocus(newLocus: Long, newReads: Seq[DecadentRead]) = {
    val reusableElements = elements.filter(_.read.record.overlapsReferencePosition(newLocus))
    val updatedElements = reusableElements.map(_.atGreaterLocus(locus))
    val newElements = newReads.map(PileupElement(_, newLocus))
    Pileup(updatedElements ++ newElements)
  }

}
object Pileup {
  def pileupsAtLoci[T](loci: Iterator[Long], readSource: Long => Seq[DecadentRead]): Iterator[Pileup] = {
    val initialEmpty = Pileup(Seq.empty)
    val iterator = loci.scanLeft(initialEmpty)((pileup: Pileup, locus: Long) => {
      val newReads = readSource(locus)
      pileup.atGreaterLocus(locus, newReads)
    })
    iterator.next()  // Discard first element, the initial empty pileup.
    iterator
  }
  def apply(reads: Seq[DecadentRead], locus: Long): Pileup = {
    val elements = reads.filter(_.record.overlapsReferencePosition(locus)).map(PileupElement(_, locus))
    Pileup(elements)
  }
}


case class PileupElement(
  read: DecadentRead,
  locus: Long,
  readPosition: Long,
  cigar: Cigar,
  indexInCigarElements: Long,
  indexWithinCigarElement: Long) {
  
  assume(locus >= read.record.start)
  assume(locus <= read.record.end)
  assume(read.record.mdTag.isDefined)

  lazy val sequenceRead: String = {
    if (isDeletion)
      ""
    else if (isMatch || isMismatch)
      read.record.getSequence.charAt(readPosition).toString
    else if (isInsertion)
      read.record.getSequence.subSequence(readPosition, readPosition + cigarElement.getLength - indexWithinCigarElement)
    else
      throw new AssertionError("Not a match, mismatch, deletion, or insertion")
  }

  lazy val cigarElement = cigar.getCigarElement(indexInCigarElements)
  lazy val isInsertion = cigarElement == CigarOperator.INSERTION
  lazy val isDeletion = cigarElement == CigarOperator.DELETION
  lazy val isMismatch = CigarOperator.MATCH_OR_MISMATCH && !read.record.mdTag.get.isMatch(locus)
  lazy val isMatch = CigarOperator.MATCH_OR_MISMATCH && read.record.mdTag.get.isMatch(locus)

  def atGreaterLocus(newLocus: Long): PileupElement = {
    if (newLocus == locus)
      this
    else {
      assume(newLocus > locus)
      assume(newLocus <= read.record.end)
      val desiredReferenceOffset = newLocus - read.record.getStart

      var currentReadPosition = readPosition - indexWithinCigarElement
      var currentReferenceOffset = locus - read.record.start - indexWithinCigarElement
      var cigarIndex = indexInCigarElements
      var prevReadPosition = -1
      var prevReferenceOffset = -1
      while (currentReferenceOffset < desiredReferenceOffset) {
        prevReadPosition = currentReadPosition
        prevReferenceOffset = currentReferenceOffset
        val element = cigar.getCigarElement(cigarIndex)
        if (element.getOperator.consumesReadBases) currentReadPosition += element.getLength
        if (element.getOperator.consumesReferenceBases) currentReferenceOffset += element.getLength
        cigarIndex += 1
      }
      assert(desiredReferenceOffset < currentReferenceOffset)
      assert(desiredReferenceOffset >= prevReferenceOffset)
      val newIndexInCigarElements = cigarIndex - 1
      val newIndexWithinCigarElement = desiredReferenceOffset - prevReferenceOffset
      val newReadPosition = prevReadPosition + newIndexWithinCigarElement
      Pileup(
        read,
        newLocus,
        newReadPosition,
        cigar,
        newIndexInCigarElements,
        newIndexWithinCigarElement
      )
    }
  }
}
object PileupElement {
  def apply(read: DecadentRead, locus: Long) = {
    val cigar = TextCigarCodec.getSingleton.decode(read.record.getCigar)
    val mdTag = MdTag(read.record.getMismatchingPositions.toString, read.record.getStart)
    PileupElement(
      read = read,
      locus = read.record.getStart,
      readPosition = 0,
      cigar = cigar,
      indexInCigarElements = 0,
      indexWithinCigarElement = 0).atGreaterLocus(locus))
  }
}


