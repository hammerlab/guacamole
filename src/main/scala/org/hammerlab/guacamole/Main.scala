package org.hammerlab.guacamole

import java.util.logging.Level

import grizzled.slf4j.Logging
import org.bdgenomics.adam.util.ParquetLogger
import org.hammerlab.commands.Command
import org.hammerlab.guacamole.commands._

/**
 * Guacamole main class.
 */
object Main
  extends Logging {

  /**
   * Commands (e.g. variant caller implementations) that are part of Guacamole. If you add a new command, update this.
   */
  private val commands: Seq[Command[_]] = List(
    GermlineAssemblyCaller.Caller,
    SomaticStandard.Caller,
    VariantSupport.Caller,
    VAFHistogram.Caller,
    SomaticJoint.Caller,
    PartitionLoci,
    PartitionReads
  )

  private def printUsage() = {
    println("Usage: java ... <command> [other args]\n")
    println("Available commands:")
    for { command ← commands } {
      println("%25s: %s".format(command.name, command.description))
    }
    println("\nTry java ... <command> -h for help on a particular variant caller.")
  }

  /**
   * Entry point into Guacamole.
   *
   * @param args command line arguments
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      printUsage()
      System.exit(1)
    }

    val commandName = args(0)
    commands.find(_.name == commandName) match {
      case Some(command) ⇒
        logger.info(s"Guacamole starting: $commandName")
        ParquetLogger.hadoopLoggerLevel(Level.SEVERE) // Quiet parquet logging.
        command.run(args.drop(1))
      case None ⇒
        logger.error(s"Unknown variant caller: $commandName")
        printUsage()
        System.exit(1)
    }
  }
}
