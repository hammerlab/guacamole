package org.hammerlab.guacamole.commands

import org.hammerlab.cli.args4j.{ Args, SparkCommand }
import org.hammerlab.guacamole.kryo.Registrar

abstract class GuacCommand[T <: Args: Manifest]
  extends SparkCommand[T] {
  registrar[Registrar]
}
