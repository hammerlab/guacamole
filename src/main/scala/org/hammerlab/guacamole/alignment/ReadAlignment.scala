package org.hammerlab.guacamole.alignment

import org.hammerlab.guacamole.alignment.AlignmentState.AlignmentState

object AlignmentState extends Enumeration {
  type AlignmentState = Value
  val Match, Mismatch, Insertion, Deletion = Value

  def isGapAlignment(state: AlignmentState) = {
    state == AlignmentState.Insertion || state == AlignmentState.Deletion
  }
}

/**
 * ReadAlignment stores a sequence of states describing the alignment of a reads to a reference
 * @param alignments Sequence of alignments
 * @param alignmentScore Score of the alignment
 */
case class ReadAlignment(alignments: Seq[AlignmentState],
                         alignmentScore: Int) {

  /**
   * Match an AlignmentState to a CIGAR operator
   * @param alignmentOperator  An alignment state
   * @return CIGAR operator corresponding to alignment state
   */
  private def cigarKey(alignmentOperator: AlignmentState): String = {
    alignmentOperator match {
      case AlignmentState.Match     => "="
      case AlignmentState.Mismatch  => "X"
      case AlignmentState.Insertion => "I"
      case AlignmentState.Deletion  => "D"
    }
  }

  /**
   * Convert a ReadAlignment to a CIGAR string
   * @return CIGAR String
   */
  def toCigar: String = {
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

    runLengthEncode(alignments.map(cigarKey))
  }
}