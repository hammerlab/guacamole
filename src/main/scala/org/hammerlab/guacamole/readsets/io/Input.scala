package org.hammerlab.guacamole.readsets.io

import org.hammerlab.guacamole.readsets.{SampleId, SampleName}

class Input(val id: SampleId, val sampleName: SampleName, val path: String) extends Serializable

object Input {
  def apply(id: SampleId, sampleName: SampleName, path: String): Input = new Input(id, sampleName, path)
  def unapply(input: Input): Option[(SampleId, SampleName, String)] = Some((input.id, input.sampleName, input.path))
}
