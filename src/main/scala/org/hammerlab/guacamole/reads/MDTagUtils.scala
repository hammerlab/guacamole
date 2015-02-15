package org.hammerlab.guacamole.reads

import debox.Buffer
import htsjdk.samtools.{Cigar, CigarOperator}
import org.bdgenomics.adam.util.MdTag
import org.hammerlab.guacamole.Bases

import scala.collection.JavaConversions

object MDTagUtils {

  /**
   * Adopted from ADAM's mdTag.getReference to operate on Seq[Byte] instead of strings
   * Also we use a Buffer from spire here instead to avoid an additional array allocation and ensure
   * we return an Array[Byte]
   *
   * Given a mdtag, read sequence, cigar, and a reference start position, returns the reference.
   *
   * @param readSequence The base sequence of the read.
   * @param cigar The cigar for the read.
   * @param referenceFrom The starting point of this read alignment vs. the reference.
   * @return A sequence of bytes corresponding to the reference overlapping this read.
   */
  def getReference(mdTag: MdTag, readSequence: Seq[Byte], cigar: Cigar, referenceFrom: Long, allowNBase: Boolean): Seq[Byte] = {

    var referencePos = mdTag.start
    var readPos = 0
    var reference: Buffer[Byte] = Buffer.empty

    // loop over all cigar elements
    JavaConversions.asScalaBuffer(cigar.getCigarElements).foreach(cigarElement => {
      cigarElement.getOperator match {
        case CigarOperator.M | CigarOperator.EQ | CigarOperator.X => {
          // if we are a match, loop over bases in element
          for (i <- 0 until cigarElement.getLength) {
            // if a mismatch, get from the mismatch set, else pull from read
            mdTag.mismatches.get(referencePos) match {
              case Some(base) => reference += base.toByte
              case _          => reference += readSequence(readPos).toByte
            }

            readPos += 1
            referencePos += 1
          }
        }
        case CigarOperator.N if allowNBase => {
          for (i <- 0 until cigarElement.getLength) {
            // if a mismatch, get from the mismatch set, else pull from read
            reference += readSequence(readPos)
            readPos += 1
            referencePos += 1
          }
        }
        case CigarOperator.D => {
          // if a delete, get from the delete pool
          for (i <- 0 until cigarElement.getLength) {
            reference += {
              mdTag.deletions.get(referencePos) match {
                case Some(base) => base.toByte
                case _          => throw new IllegalStateException("Could not find deleted base at cigar offset " + i)
              }
            }

            referencePos += 1
          }
        }
        case _ => {
          // ignore inserts
          if (cigarElement.getOperator.consumesReadBases) {
            readPos += cigarElement.getLength
          }
          if (cigarElement.getOperator.consumesReferenceBases) {
            throw new IllegalArgumentException("Cannot handle operator: " + cigarElement.getOperator)
          }
        }
      }
    })

    reference.toArray
  }

  def getReference(read: MappedRead, allowNBase: Boolean): Seq[Byte] = {
    getReference(read.mdTag, read.sequence, read.cigar, read.start, allowNBase)
  }

  /**
   * Rebuilds the reference from a set of overlapping and sorted reads
   * Fill in N if there is a gap in the reads
   *
   * @param sortedReads Set of overlapping and sorted reads that are mapped to the reference
   * @return A sequence of bytes corresponding to the reference overlapping these read.
   */
  def getReference(sortedReads: Seq[MappedRead]): Array[Byte] = {
    // assume reads are sorted
    val referenceSeq: Buffer[Byte] = Buffer.empty
    val readsBuffer = sortedReads.iterator.buffered
    var currentLocus = readsBuffer.head.start
    while (readsBuffer.hasNext) {
      val read = readsBuffer.next()
      val offsetInRead = currentLocus - read.start
      if (offsetInRead < 0) {
        //Fill in gap with N
        (currentLocus until read.start).foreach(_ => referenceSeq += Bases.N)
        currentLocus = read.start
      }

      // Find last read that overlaps the current locus
      if (!readsBuffer.hasNext || currentLocus < readsBuffer.head.start) {
        val readReferenceSequence = MDTagUtils.getReference(read, allowNBase = true)
        referenceSeq ++= readReferenceSequence.drop(offsetInRead.toInt)
        currentLocus = read.end
      }
    }
    referenceSeq.toArray
  }

}
