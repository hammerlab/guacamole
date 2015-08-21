package org.hammerlab.guacamole.alignment

import breeze.linalg.DenseMatrix
import org.hammerlab.guacamole.alignment.AlignmentState.{ AlignmentState, isGapAlignment }

object AffineGapPenaltyAlignment {

  type Path = (Seq[AlignmentState], Double)

  /**
   * Produces an alignment of an input sequence against a reference sequence
   *
   * @param sequence Input sequence to align
   * @param reference Reference sequence to align against
   * @param mismatchProbability Penalty to having a mismatch in the alignment
   * @param openGapProbability Penalty to start an insertion or deletion in the alignment
   * @param closeGapProbability Penalty to end in an insertion or deletion in the alignment
   * @return An alignment path and score
   */
  def align(sequence: Seq[Byte],
            reference: Seq[Byte],
            mismatchProbability: Double = math.exp(-4),
            openGapProbability: Double = math.exp(-6),
            closeGapProbability: Double = 1 - math.exp(-1)): ReadAlignment = {
    // TODO: What are the best defaults?
    // BWA defaults:
    // -log mismatchProbability = 4
    // -log openGapProbability = 6
    // -log (1 - closeGapProbability) = 1

    val alignments =
      scoreAlignmentPaths(
        sequence,
        reference,
        mismatchProbability,
        openGapProbability,
        closeGapProbability
      )

    val (path, score) = (for (i <- 0 to reference.length) yield { alignments(sequence.length, i) }).minBy(_._2)
    ReadAlignment(path, score.toInt)
  }

  def scoreAlignmentPaths(sequence: Seq[Byte],
                          reference: Seq[Byte],
                          mismatchProbability: Double,
                          openGapProbability: Double,
                          closeGapProbability: Double): DenseMatrix[Path] = {

    val logMismatchPenalty = -math.log(mismatchProbability)

    val logOpenGapPenalty = -math.log(openGapProbability)
    val noGapPenalty = -math.log(1 - openGapProbability)

    val logCloseGapPenalty = -math.log(closeGapProbability)
    val logContinueGapPenalty = -math.log(1 - closeGapProbability)

    val sequenceLength = sequence.length
    val referenceLength = reference.length

    // alignments is an (M+1) x (N+1) matrix
    // M is the length of the sequence and N is the length of the reference
    // Element (i,j) represents the best alignment of the first `i` bases of sequence to the first `j` bases of reference.
    val alignments = new DenseMatrix[Path](sequenceLength + 1, referenceLength + 1)
    alignments(0, 0) = (Nil, 0)

    def transitionPenalty(nextState: AlignmentState, previousStateOpt: Option[AlignmentState], isEndState: Boolean) = {

      val openGap = !previousStateOpt.exists(_ == nextState) && isGapAlignment(nextState)
      val closeGap = previousStateOpt.exists(previousState => nextState != previousState && isGapAlignment(previousState))
      val continueGap = previousStateOpt.exists(_ == nextState) && isGapAlignment(nextState)
      val mismatch = nextState == AlignmentState.Mismatch

      (if (openGap) logOpenGapPenalty else 0) +
        (if (closeGap) logCloseGapPenalty else 0) +
        (if (continueGap) logContinueGapPenalty else if (mismatch) noGapPenalty + logMismatchPenalty else noGapPenalty) +
        (if (isEndState && isGapAlignment(nextState)) logCloseGapPenalty else 0)
    }

    for {
      referenceIdx <- 0 to referenceLength
      sequenceIdx <- 0 to sequenceLength
      if referenceIdx > 0 || sequenceIdx > 0
    } {
      // Given the change in position, is the transition a gap or match/mismatch
      def classifyTransition(prevSeqPos: Int, prevRefPos: Int): AlignmentState = {
        if (sequenceIdx == prevSeqPos) {
          AlignmentState.Deletion
        } else if (referenceIdx == prevRefPos) {
          AlignmentState.Insertion
        } else if (sequence(sequenceIdx - 1) != reference(referenceIdx - 1)) {
          AlignmentState.Mismatch
        } else {
          AlignmentState.Match
        }
      }

      val possiblePreviousStates =
        Seq(
          (sequenceIdx - 1, referenceIdx),
          (sequenceIdx, referenceIdx - 1),
          (sequenceIdx - 1, referenceIdx - 1)
        ).filter {
            case (sI, rI) => sI >= 0 && rI >= 0
          }

      // Compute the transition costs based on the gap penalties
      val nextPaths: Seq[Path] = possiblePreviousStates.map {
        case (prevSeqPos, prevRefPos) => {
          val nextState = classifyTransition(prevSeqPos, prevRefPos)

          val (prevPath, prevScore) = alignments(prevSeqPos, prevRefPos)
          val prevStateOpt = prevPath.lastOption

          val isEndState = sequenceIdx == sequenceLength

          val transitionCost = transitionPenalty(nextState, prevStateOpt, isEndState = isEndState)
          (prevPath :+ nextState, prevScore + transitionCost)
        }
      }

      alignments(sequenceIdx, referenceIdx) = nextPaths.minBy(_._2)
    }

    alignments
  }
}
