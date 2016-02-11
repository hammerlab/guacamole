package org.hammerlab.guacamole.alignment

import htsjdk.samtools.{ Cigar, TextCigarCodec }
import org.hammerlab.guacamole.alignment.AlignmentState.AlignmentState

object AlignmentState extends Enumeration {
  type AlignmentState = Value
  val Match, Mismatch, Insertion, Deletion = Value

  def isGapAlignment(state: AlignmentState) = {
    state == AlignmentState.Insertion || state == AlignmentState.Deletion
  }

  /**
   * Match an AlignmentState to a CIGAR operator
   * @param alignmentOperator  An alignment state
   * @return CIGAR operator corresponding to alignment state
   */
  def cigarKey(alignmentOperator: AlignmentState): String = {
    alignmentOperator match {
      case AlignmentState.Match     => "="
      case AlignmentState.Mismatch  => "X"
      case AlignmentState.Insertion => "I"
      case AlignmentState.Deletion  => "D"
    }
  }
}

/**
 *
 * @param alignments Sequence of alignments
 * @param refStartIdx Start of the alignment in the reference sequence
 * @param refEndIdx End of the alignment (inclusive) in the reference sequence
 * @param alignmentScore Score of the alignment based on the mismatch and gap penalties
 */
case class ReadAlignment(alignments: Seq[AlignmentState],
                         refStartIdx: Int,
                         refEndIdx: Int,
                         alignmentScore: Int) {

  /**
   * Convert a ReadAlignment to a CIGAR string
   * @return CIGAR String
   */
  def toCigarString: String = {
    def runLengthEncode(operators: Seq[String]): String = {
      var lastOperator = operators.head
      var i = 1
      val rle = new StringBuffer()
      var currentRun = 1
      while (i < operators.size) {
        if (operators(i) == lastOperator) {
          currentRun += 1
        } else {
          rle.append(currentRun.toString + lastOperator)
          currentRun = 1
        }
        lastOperator = operators(i)
        i += 1
      }
      rle.append(currentRun.toString + lastOperator)
      rle.toString
    }

    runLengthEncode(alignments.map(alignment => AlignmentState.cigarKey(alignment)))
  }

  def toCigar: Cigar = {
    val cigarString = this.toCigarString
    TextCigarCodec.decode(cigarString)
  }
}
