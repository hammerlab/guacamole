package org.hammerlab.guacamole.jointcaller.pileup_summarization

import org.hammerlab.genomics.readsets.PerSample
import org.hammerlab.guacamole.jointcaller.Samples

/**
 * Collection of per-sample PileupStats instances plus pooled normal and tumor DNA PileupStats.
 *
 * Used as a convenient way to pass several PileupStats instances around.
 *
 */
case class MultiplePileupStats(inputs: Samples, singleSampleStats: PerSample[PileupStats]) {
  val referenceSequence = singleSampleStats.head.referenceSequence
  val normalDNAPooled =
    PileupStats(
      inputs
        .normalDNA
        .flatMap(input ⇒ singleSampleStats(input.id).elements),
      referenceSequence
    )

  val tumorDNAPooled =
    PileupStats(
      inputs
        .tumorDNA
        .flatMap(
          input ⇒
            singleSampleStats(input.id).elements
        ),
      referenceSequence
    )
}
