package org.hammerlab.guacamole

import java.nio.file.spi.FileSystemProvider
import java.util.logging.Level

import grizzled.slf4j.Logging
import org.bdgenomics.adam.util.ParquetLogger
import org.hammerlab.commands.Command
import org.hammerlab.guacamole.commands._
import org.hammerlab.guacamole.logging.LoggingUtils.progress

/**
 * Guacamole main class.
 */
object Main extends Logging {

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
    commands.foreach { caller ⇒
      println("%25s: %s".format(caller.name, caller.description))
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

    /** Hack to pick up [[FileSystemProvider]] implementations; see https://issues.scala-lang.org/browse/SI-10247. */
    val scl = classOf[ClassLoader].getDeclaredField("scl")
    scl.setAccessible(true)
    val prevClassLoader = ClassLoader.getSystemClassLoader
    scl.set(null, Thread.currentThread().getContextClassLoader)
    FileSystemProvider.installedProviders()
    scl.set(null, prevClassLoader)

    val commandName = args(0)
    commands.find(_.name == commandName) match {
      case Some(command) ⇒ {
        progress("Guacamole starting.")
        ParquetLogger.hadoopLoggerLevel(Level.SEVERE) // Quiet parquet logging.
        command.run(args.drop(1))
      }
      case None ⇒ {
        println("Unknown variant caller: %s".format(commandName))
        printUsage()
        System.exit(1)
      }
    }
  }
}
