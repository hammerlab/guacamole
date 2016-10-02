package org.hammerlab.guacamole.alignment

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
