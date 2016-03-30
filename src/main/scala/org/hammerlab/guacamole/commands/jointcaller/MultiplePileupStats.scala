package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.DistributedUtil.PerSample

case class MultiplePileupStats(inputs: InputCollection, singleSampleStats: PerSample[PileupStats]) {
  val referenceSequence = singleSampleStats.head.referenceSequence
  val normalDNAPooled = PileupStats(inputs.normalDNA.flatMap(input => singleSampleStats(input.index).elements), referenceSequence)
  val tumorlDNAPooled = PileupStats(inputs.tumorDNA.flatMap(input => singleSampleStats(input.index).elements), referenceSequence)
}

