package org.hammerlab.guacamole.commands

import org.hammerlab.commands.{ Args, SparkCommand }

abstract class GuacCommand[T <: Args: Manifest] extends SparkCommand[T] {
  override def defaultRegistrar: String = "org.hammerlab.guacamole.kryo.Registrar"
}
