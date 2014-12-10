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

import scala.collection.mutable

/**
 *  It's often useful to output counts of certain events that occur in RDD operations. The functions that operate on
 *  RDDs, however, can't print these counters since the transformations they apply to the RDDs may not execute until
 *  much later (Spark RDD transformations are lazy). This class provides a simple mechanism to register a message to be
 *  outputted at program termination, after all RDD transformations have been forced.
 *
 */
class DelayedMessages {
  private val messages = new mutable.Queue[() => String]()

  /** Enqueue a () => String function whose value will be printed at program termination. */
  def say(message: () => String): Unit = messages.enqueue(message)

  /** Print to stdout all queued messages, and empty the queue. */
  def print(): Unit = {
    println("*** Delayed Messages ***")
    while (messages.nonEmpty) {
      val message = messages.dequeue()
      println(message())
    }
  }
}
object DelayedMessages {
  /** Create a new DelayedMessages instance */
  def apply(): DelayedMessages = new DelayedMessages()

  /**
   * To avoid passing a DelayedMessages instance to every function that may need one, we provide a singleton default
   * instance for convenience.
   */
  val default = DelayedMessages()
}
