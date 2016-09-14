package org.hammerlab.guacamole.commands

import grizzled.slf4j.Logging
import org.bdgenomics.utils.cli.Args4j

/**
 * Interface for running a command from command line arguments.
 *
 * We give the variant callers and other commands control of execution. Guacamole is a toolbox of common functionality
 * for these to use as appropriate.
 *
 * If you add a new command, you should also update the commands list in Guacamole.scala to include it.
 */
abstract class Command[T <: Args: Manifest] extends Serializable with Logging {
  /** The name of the command, as it will be specified on the command line. */
  def name: String

  /** A short description of the command, for display in the usage info on the command line. */
  def description: String

  /**
   * Run the command.
   *
   * @param args the command line arguments, with the first one chopped off. The first argument specifies which
   *             command to run, and is therefore already consumed by Guacamole.
   */
  def run(args: Array[String]): Unit = run(Args4j[T](args))
  def run(args: String*): Unit = run(args.toArray)

  def run(args: T): Unit
}
