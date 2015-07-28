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

import org.apache.spark.{ SparkContext, Logging }
import org.bdgenomics.utils.cli.{ Args4j, Args4jBase }

/**
 * Interface for running a command from command line arguments.
 *
 * We give the variant callers and other commands control of execution. Guacamole is a toolbox of common functionality
 * for these to use as appropriate.
 *
 * If you add a new command, you should also update the commands list in Guacamole.scala to include it.
 *
 */
abstract class Command[T <% Args4jBase: Manifest] extends Serializable with Logging {
  /** The name of the command, as it will be specified on the command line. */
  val name: String

  /** A short description of the command, for display in the usage info on the command line. */
  val description: String

  /**
   * Run the command.
   *
   * @param args the command line arguments, with the first one chopped off. The first argument specifies which
   *             command to run, and is therefore already consumed by Guacamole.
   */
  def run(args: Array[String]): Any = run(Args4j[T](args))

  def run(args: T): Any
}

abstract class SparkCommand[T <% Args4jBase: Manifest] extends Command[T] {
  override def run(args: T): Any = {
    val sc = Common.createSparkContext(appName = Some(name))
    try {
      run(args, sc)
    } finally {
      sc.stop()
    }
  }

  def run(args: T, sc: SparkContext): Any

  /** This method is useful for unit tests. */
  def run(sc: SparkContext, args: String*): Any = {
    run(Args4j[T](args.toArray), sc)
  }
}
