package org.hammerlab.guacamole.alignment

import breeze.linalg.DenseVector
import org.hammerlab.guacamole.alignment.AlignmentState.{ AlignmentState, isGapAlignment }

object AffineGapPenaltyAlignment {

  type Path = (Int, List[AlignmentState], Double)

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

    val alignment =
      scoreAlignmentPaths(
        sequence,
        reference,
        mismatchProbability,
        openGapProbability,
        closeGapProbability
      )

    val ((refEndIdx, path, score), refStartIdx) =
      (for (i <- 0 to reference.length) yield {
        (alignment(i), i)
      }).minBy(_._1._3)

    ReadAlignment(path, refStartIdx, refEndIdx, score.toInt)
  }

  def scoreAlignmentPaths(sequence: Seq[Byte],
                          reference: Seq[Byte],
                          mismatchProbability: Double,
                          openGapProbability: Double,
                          closeGapProbability: Double): DenseVector[Path] = {

    val logMismatchPenalty = -math.log(mismatchProbability)

    val logOpenGapPenalty = -math.log(openGapProbability)
    val noGapPenalty = -math.log(1 - openGapProbability)

    val logCloseGapPenalty = -math.log(closeGapProbability)
    val logContinueGapPenalty = -math.log(1 - closeGapProbability)

    val sequenceLength = sequence.length
    val referenceLength = reference.length

    var lastSequenceAlignment = new DenseVector[Path](referenceLength + 1)
    for {
      refIdx <- 0 to referenceLength
    } {
      lastSequenceAlignment(refIdx) = (refIdx, List.empty, 0)
    }
    var currentSequenceAlignment = new DenseVector[Path](referenceLength + 1)

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

    // Alignment is performed from the end the sequence backwards
    // This ensure that insertion and deletion alignments occur in the left-most position

    var sequenceIdx = sequenceLength - 1
    while (sequenceIdx >= 0) {
      var referenceIdx = referenceLength
      while (referenceIdx >= 0) {

        // Given the change in position, is the transition a gap or match/mismatch
        def classifyTransition(prevSeqPos: Int, prevRefPos: Int): AlignmentState = {
          if (sequenceIdx == prevSeqPos) {
            AlignmentState.Deletion
          } else if (referenceIdx == prevRefPos) {
            AlignmentState.Insertion
          } else if (sequence(sequenceIdx) != reference(referenceIdx)) {
            AlignmentState.Mismatch
          } else {
            AlignmentState.Match
          }
        }

        val possiblePreviousStates =
          Seq(
            (sequenceIdx + 1, referenceIdx),
            (sequenceIdx, referenceIdx + 1),
            (sequenceIdx + 1, referenceIdx + 1)
          ).filter {
              case (sI, rI) => sI <= sequenceLength && rI <= referenceLength // Filter positions before the start of either sequence
            }

        // Compute the transition costs based on the gap penalties
        val nextPaths: Seq[Path] = possiblePreviousStates.map {
          case (prevSeqPos, prevRefPos) => {
            val nextState = classifyTransition(prevSeqPos, prevRefPos)

            val (prevRefStartIdx, prevPath, prevScore) =
              nextState match {
                case AlignmentState.Deletion  => currentSequenceAlignment(referenceIdx + 1)
                case AlignmentState.Insertion => lastSequenceAlignment(referenceIdx)
                case _ => {
                  lastSequenceAlignment(referenceIdx + 1)
                }
              }

            val prevStateOpt = prevPath.headOption

            val isEndState = sequenceIdx == 0

            val transitionCost = transitionPenalty(nextState, prevStateOpt, isEndState = isEndState)
            (prevRefStartIdx, nextState :: prevPath, prevScore + transitionCost)
          }
        }

        currentSequenceAlignment(referenceIdx) = nextPaths.minBy(_._3)
        referenceIdx -= 1
      }
      // Save current sequence position alignment position
      lastSequenceAlignment = currentSequenceAlignment

      // Clear alignment information before next sequence element
      currentSequenceAlignment = new DenseVector[Path](referenceLength + 1)
      sequenceIdx -= 1
    }

    lastSequenceAlignment
  }
}
