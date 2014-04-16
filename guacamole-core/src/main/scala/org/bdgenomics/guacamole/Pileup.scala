package org.bdgenomics.guacamole

import org.bdgenomics.adam.rich.DecadentRead
import net.sf.samtools.{ CigarElement, Cigar, CigarOperator, TextCigarCodec }
import org.bdgenomics.adam.util.MdTag
import scala.collection.parallel.mutable

case class Pileup(elements: Seq[Pileup.Element]) {
  lazy val head = elements.head
  lazy val locus: Long = head.locus
  lazy val referenceName: String = head.read.record.referenceName.toString

  assume(elements.forall(_.read.record.referenceName == referenceName), "Reads in pileup have mismatching reference names")
  assume(elements.forall(_.locus == locus), "Reads in pileup have mismatching loci")

  lazy val referenceBase: Char = {
    head.read.record.mdTag.get.getReference(head.read.record).charAt((head.locus - head.read.record.start).toInt)
  }

  lazy val bySample: Map[String, Pileup] = {
    elements.groupBy(_.read.record.recordGroupSample.toString).map({ case (sample, elements) => (sample, Pileup(elements)) })
  }

  def atGreaterLocus(newLocus: Long, newReads: Iterator[DecadentRead]) = {
    val reusableElements = elements.filter(_.read.record.overlapsReferencePosition(newLocus).getOrElse(false))
    val updatedElements = reusableElements.map(_.atGreaterLocus(locus))
    val newElements = newReads.map(Pileup.Element(_, newLocus))
    Pileup(updatedElements ++ newElements)
  }

}
object Pileup {
  def pileupsAtLoci[T](loci: Iterator[Long], readSource: Long => Iterator[DecadentRead]): Iterator[Pileup] = {
    val initialEmpty = Pileup(Seq.empty)
    val iterator = loci.scanLeft(initialEmpty)((pileup: Pileup, locus: Long) => {
      val newReads = readSource(locus)
      pileup.atGreaterLocus(locus, newReads)
    })
    iterator.next() // Discard first element, the initial empty pileup.
    iterator
  }
  def apply(reads: Seq[DecadentRead], locus: Long): Pileup = {
    val elements = reads.filter(_.record.overlapsReferencePosition(locus).getOrElse(false)).map(Element(_, locus))
    Pileup(elements)
  }

  case class Element(
      read: DecadentRead,
      locus: Long,
      readPosition: Long,
      cigar: Cigar,
      indexInCigarElements: Long,
      indexWithinCigarElement: Long) {

    assume(locus >= read.record.getStart)
    assume(locus <= read.record.end.get)
    assume(read.record.mdTag.isDefined)

    lazy val sequenceRead: String = {
      if (isDeletion)
        ""
      else if (isMatch || isMismatch)
        read.record.getSequence.charAt(readPosition.toInt).toString
      else if (isInsertion)
        read.record.getSequence.toString.subSequence(
          readPosition.toInt,
          readPosition.toInt + cigarElement.getLength - indexWithinCigarElement.toInt).toString
      else
        throw new AssertionError("Not a match, mismatch, deletion, or insertion")
    }

    lazy val cigarElement = cigar.getCigarElement(indexInCigarElements.toInt)
    lazy val isInsertion = cigarElement == CigarOperator.INSERTION
    lazy val isDeletion = cigarElement == CigarOperator.DELETION
    lazy val isMismatch = cigarElement == CigarOperator.MATCH_OR_MISMATCH && !read.record.mdTag.get.isMatch(locus)
    lazy val isMatch = cigarElement == CigarOperator.MATCH_OR_MISMATCH && read.record.mdTag.get.isMatch(locus)

    def atGreaterLocus(newLocus: Long): Element = {
      if (newLocus == locus)
        this
      else {
        assume(newLocus > locus)
        assume(newLocus <= read.record.end.get)
        val desiredReferenceOffset = newLocus - read.record.getStart

        var currentReadPosition = readPosition - indexWithinCigarElement
        var currentReferenceOffset = locus - read.record.start - indexWithinCigarElement
        var cigarIndex = indexInCigarElements
        var prevReadPosition: Long = -1
        var prevReferenceOffset: Long = -1
        while (currentReferenceOffset < desiredReferenceOffset) {
          prevReadPosition = currentReadPosition
          prevReferenceOffset = currentReferenceOffset
          val element = cigar.getCigarElement(cigarIndex.toInt)
          if (element.getOperator.consumesReadBases) currentReadPosition += element.getLength
          if (element.getOperator.consumesReferenceBases) currentReferenceOffset += element.getLength
          cigarIndex += 1
        }
        assert(desiredReferenceOffset < currentReferenceOffset)
        assert(desiredReferenceOffset >= prevReferenceOffset)
        val newIndexInCigarElements = cigarIndex - 1
        val newIndexWithinCigarElement = desiredReferenceOffset - prevReferenceOffset
        val newReadPosition = prevReadPosition + newIndexWithinCigarElement
        Element(
          read,
          newLocus,
          newReadPosition,
          cigar,
          newIndexInCigarElements,
          newIndexWithinCigarElement)
      }
    }
  }
  object Element {
    def apply(read: DecadentRead, locus: Long): Element = {
      val cigar = TextCigarCodec.getSingleton.decode(read.record.getCigar.toString)
      val mdTag = MdTag(read.record.getMismatchingPositions.toString, read.record.getStart)
      Element(
        read = read,
        locus = read.record.getStart,
        readPosition = 0,
        cigar = cigar,
        indexInCigarElements = 0,
        indexWithinCigarElement = 0).atGreaterLocus(locus)
    }
  }
}

