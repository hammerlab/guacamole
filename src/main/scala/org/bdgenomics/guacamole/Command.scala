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

package org.bdgenomics.guacamole

/**
 *
 * Interface for running a variant caller from command line arguments.
 *
 * We give the variant commands control of execution. Guacamole is just a toolbox of common functionality for the variant
 * commands to use as appropriate.
 *
 * If you add a new command, you should also update the [[Guacamole.commands]] list in Guacamole.scala to
 * include it.
 *
 */
trait Command {
  /** The name of the variant caller, as it will be specified on the command line. */
  val name: String

  /** A short description of the variant caller, for display in the usage info on the command line. */
  val description: String

  /**
   * Run the variant caller.
   *
   * @param args the command line arguments, with the first one chopped off. The first argument specifies what variant
   *             caller to call, and is therefore already consumed by Guacamole.
   */
  def run(args: Array[String]): Unit
}
