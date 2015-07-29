/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole.commands

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.util.{ TestUtil, GuacFunSuite }
import org.scalatest.Matchers

class EvalCommandSuite extends GuacFunSuite with Matchers {

  val chrM = TestUtil.testDataPath("chrM.sorted.bam")
  val gasvExample = TestUtil.testDataPath("gasv_example_both_mapped_and_unmapped.bam")

  def run(args: String*): Seq[String] = {
    EvalCommand.Caller.run(sc, args: _*).asInstanceOf[RDD[String]].collect()
  }
  def runChrM(args: String*): Seq[String] = {
    val fullArgs: Seq[String] = Seq(chrM, "::") ++ args ++ Seq("--loci", "chrM:5000-5005", "--out", "none", "-q")
    run(fullArgs: _*)
  }

  def runGasv(args: String*): Seq[String] = {
    val fullArgs: Seq[String] = Seq(gasvExample, "::") ++ args ++ Seq("--domain", "reads", "--out", "none", "-q")
    run(fullArgs: _*)
  }

  /*
  sparkTest("basic") {
    runChrM("locus") should equal((5000 until 5005).map(_.toString))
    runChrM("locus * 2") should equal(Seq("10000", "10002", "10004", "10006", "10008"))
  }

  sparkTest("includes") {
    runChrM(
      "--include-code", "function locusTimesTwo() {return locus * 2;}",
      "--include-code", "function locusTimesNegativeTwo() {return locus * -2; }",
      "locusTimesNegativeTwo()") should equal(
        Seq("-10000", "-10002", "-10004", "-10006", "-10008"))

    runChrM(
      "--include-code", "function locusTimesTwo() {return locus * 2;}",
      "--include-code", "function locusTimesNegativeTwo() {return locus * -2; }",
      "locusTimesNegativeTwo()",
      "locusTimesTwo()") should equal(
        Seq("-10000, 10000", "-10002, 10002", "-10004, 10004", "-10006, 10006", "-10008, 10008"))
  }

  sparkTest("pileups") {
    runChrM("locus", "pileup().count('')", "pileup().count('element.isMatch()')") should equal(Seq(
      "5000, 161, 161", "5001, 159, 158", "5002, 155, 151", "5003, 156, 155", "5004, 157, 153"
    ))
  }
  */

  sparkTest("reads") {
    runChrM("read.start()", "read.end()", "--domain", "reads").size should equal(38461)

    runGasv("read.isMapped()").groupBy(identity).mapValues(_.size) should equal(
      Map("true" -> 197376, "false" -> (231375 - 197376)))

  }
}
