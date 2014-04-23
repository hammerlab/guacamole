/*
 * Copyright (c) 2013-2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.guacamole

import org.apache.spark.Logging
import java.util.logging.Level
import org.bdgenomics.guacamole.callers.ThresholdVariantCaller
import org.bdgenomics.adam.util.ParquetLogger
import org.bdgenomics.guacamole.Util.progress
import scala.Some

/**
 * Guacamole main class.
 */
object Guacamole extends Logging {

  /**
   * Commands (i.e. variant caller implementations) that are part of Guacamole. If you add a new variant caller, update
   * this list.
   */
  private val commands: Seq[Command] = List(
    ThresholdVariantCaller)

  private def printUsage() = {
    println("Usage: java ... <command> [other args]\n")
    println("Available variant calling commands:")
    commands.foreach(caller => {
      println("%10s: %s".format(caller.name, caller.description))
    })
    println("\nTry java ... <command> -h for help on a particular variant caller.")
  }

  /**
   * Entry point into Guacamole.
   * @param args command line arguments
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      printUsage()
      System.exit(1)
    }
    val commandName = args(0)
    commands.find(_.name == commandName) match {
      case Some(command) => {
        progress("Guacamole starting.")
        ParquetLogger.hadoopLoggerLevel(Level.SEVERE) // Quiet parquet logging.
        command.run(args.drop(1))
      }
      case None => {
        println("Unknown variant caller: %s".format(commandName))
        printUsage()
        System.exit(1)
      }
    }
  }
}
