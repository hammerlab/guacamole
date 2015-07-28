package org.hammerlab.guacamole.alignment

import breeze.linalg.{ DenseMatrix, argmin }
import org.hammerlab.guacamole.alignment.AlignmentState.AlignmentState

import scala.annotation.tailrec

object AffineGapPenaltyAlignment {

  /**
   * Retraces the optimal alignment path from a matrix of scores and transitions
   *
   * @param alignmentStates sequenceLength x referenceLength matrix of alignment score at each step
   * @param alignmentScores sequenceLength x referenceLength matrix of best transition at each step
   * @param sequenceLength Length of the input sequence to align
   * @param referenceLength Length of reference sequence to align against
   * @return An alignment path and score
   */
  def findBestPath(alignmentStates: DenseMatrix[AlignmentState],
                   alignmentScores: DenseMatrix[Double],
                   sequenceLength: Int,
                   referenceLength: Int): ReadAlignment = {

    @tailrec
    def recFindBestPath(sequenceIdx: Int,
                        referenceIdx: Int,
                        currentPath: List[AlignmentState]): List[AlignmentState] = {

      val lastMove = alignmentStates(sequenceIdx, referenceIdx)
      if (sequenceIdx == 1 && referenceIdx == 1) {
        lastMove :: currentPath
      } else {
        lastMove match {
          case AlignmentState.Match | AlignmentState.Mismatch => {
            recFindBestPath(sequenceIdx - 1, referenceIdx - 1, lastMove :: currentPath)
          }
          case AlignmentState.Insertion => {
            recFindBestPath(sequenceIdx - 1, referenceIdx, lastMove :: currentPath)
          }
          case AlignmentState.Deletion => {
            recFindBestPath(sequenceIdx, referenceIdx - 1, lastMove :: currentPath)
          }
        }
      }
    }

    val alignments = recFindBestPath(sequenceLength, referenceLength, List.empty)
    ReadAlignment(alignments, argmin(alignmentScores.t(::, sequenceLength)))
  }

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
            mismatchProbability: Double = 1e-3,
            openGapProbability: Double = 1e-5,
            closeGapProbability: Double = 1e-2): ReadAlignment = {

    val sequenceLength = sequence.length
    val referenceLength = reference.length
    val (alignmentStates, alignmentScores) = scoreAlignmentPaths(sequence,
      reference,
      mismatchProbability,
      openGapProbability,
      closeGapProbability)

    findBestPath(alignmentStates, alignmentScores, sequenceLength, referenceLength)
  }

  def scoreAlignmentPaths(sequence: Seq[Byte],
                          reference: Seq[Byte],
                          mismatchProbability: Double = 1e-2,
                          openGapProbability: Double = 1e-3,
                          closeGapProbability: Double = 1e-2): (DenseMatrix[AlignmentState], DenseMatrix[Double]) = {

    val logMismatchPenalty = -math.round(math.log(mismatchProbability))
    val logOpenGapPenalty = -math.round(math.log(openGapProbability))
    val logCloseGapPenalty = -math.round(math.log(closeGapProbability))

    val logContinueGapPenalty = -math.round(math.log(1 - closeGapProbability)).toInt
    val noGapPenalty = -math.round(math.log(1 - openGapProbability)).toInt

    val sequenceLength = sequence.length
    val referenceLength = reference.length

    // alignmentScores is M x N matrix
    // M is the length of the sequence and N is the length of the reference
    val alignmentScores = DenseMatrix.fill[Double](sequenceLength + 1, referenceLength + 1) { Double.PositiveInfinity }
    val alignmentStates = new DenseMatrix[AlignmentState](sequenceLength + 1, referenceLength + 1)

    def transitionPenalty(nextState: AlignmentState, previousState: AlignmentState, isEndState: Boolean) = {

      val openGap = nextState != previousState && AlignmentState.isGapAlignment(nextState)
      val closeGap = nextState != previousState && AlignmentState.isGapAlignment(previousState)
      val continueGap = nextState == previousState && AlignmentState.isGapAlignment(nextState)
      val mismatch = nextState == AlignmentState.Mismatch

      (if (openGap) logOpenGapPenalty else 0) +
        (if (closeGap) logCloseGapPenalty else 0) +
        (if (continueGap) logContinueGapPenalty else if (mismatch) noGapPenalty + logMismatchPenalty else noGapPenalty) +
        (if (isEndState && AlignmentState.isGapAlignment(nextState)) logCloseGapPenalty else 0)
    }

    // empty case
    alignmentScores(0, 0) = 0

    for (referenceIdx <- 1 to referenceLength) {
      for (sequenceIdx <- 1 to sequenceLength) {
        // Given the change is position, is the transition a gap or match/mismatch
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

        val possiblePreviousStates = Seq(
          (sequenceIdx - 1, referenceIdx),
          (sequenceIdx, referenceIdx - 1),
          (sequenceIdx - 1, referenceIdx - 1)
        )

        // Compute the transition costs based on the gap penalties
        val stateScores: Seq[(AlignmentState, Double)] = possiblePreviousStates.map({
          case (prevSeqPos, prevRefPos) => {
            val nextState = classifyTransition(prevSeqPos, prevRefPos)
            val previousState = alignmentStates(prevSeqPos, prevRefPos)
            val previousScore = alignmentScores(prevSeqPos, prevRefPos)
            val isEndState = (sequenceIdx == sequenceLength && referenceIdx == referenceLength)

            val transitionCost = transitionPenalty(nextState, previousState, isEndState = isEndState)
            (nextState, previousScore + transitionCost)
          }
        })

        val optimalTransition = stateScores.minBy(_._2)
        alignmentStates(sequenceIdx, referenceIdx) = optimalTransition._1
        alignmentScores(sequenceIdx, referenceIdx) = optimalTransition._2
      }
    }

    (alignmentStates, alignmentScores)
  }
}
