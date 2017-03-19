package org.hammerlab.guacamole.jointcaller

import org.hammerlab.genomics.readsets.PerSample

/**
 * Convenience container for zero or more Input instances.
 */
trait SamplesI[+T <: Sample] {
  def samples: PerSample[T]
  @transient lazy val normalDNA = samples.filter(_.normalDNA)
  @transient lazy val normalRNA = samples.filter(_.normalRNA)
  @transient lazy val tumorDNA = samples.filter(_.tumorDNA)
  @transient lazy val tumorRNA = samples.filter(_.tumorRNA)
}

/**
 * Simple concrete [[SamplesI]].
 */
private case class SamplesImpl(samples: PerSample[Sample])
  extends Samples

object Samples {
  implicit def packSamples[T <: Sample](samples: PerSample[T]): Samples = SamplesImpl(samples)
  implicit def unpackSamples[T](samples: Samples): PerSample[Sample] = samples.samples
}
