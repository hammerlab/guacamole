/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole

import java.util.logging.Level

import org.apache.spark.Logging
import org.bdgenomics.adam.util.ParquetLogger
import org.hammerlab.guacamole.Common.progress
import org.hammerlab.guacamole.commands._
import org.hammerlab.guacamole.commands.jointcaller.SomaticJoint

/**
 * Guacamole main class.
 */
object Guacamole extends Logging {

  /**
   * Commands (e.g. variant caller implementations) that are part of Guacamole. If you add a new command, update this.
   */
  private val commands: Seq[Command[_]] = List(
    GermlineAssemblyCaller.Caller,
    SomaticStandard.Caller,
    SomaticMutectLike.Caller,
    VariantSupport.Caller,
    VAFHistogram.Caller,
    SomaticJoint.Caller)

  private def printUsage() = {
    println("Usage: java ... <command> [other args]\n")
    println("Available commands:")
    commands.foreach(caller => {
      println("%25s: %s".format(caller.name, caller.description))
    })
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
