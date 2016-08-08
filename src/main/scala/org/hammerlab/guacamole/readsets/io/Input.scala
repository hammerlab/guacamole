package org.hammerlab.guacamole.readsets.io

import org.hammerlab.guacamole.readsets.SampleName

class Input(val sampleName: SampleName, val path: String) extends Serializable

object Input {
  def apply(sampleName: SampleName, path: String): Input = new Input(sampleName, path)
  def unapply(input: Input): Option[(SampleName, String)] = Some((input.sampleName, input.path))
}
