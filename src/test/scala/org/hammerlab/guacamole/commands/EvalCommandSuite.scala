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

package org.hammerlab.guacamole.commands

import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.cli.Args4j
import org.hammerlab.guacamole.Guacamole
import org.hammerlab.guacamole.reads.Read
import org.hammerlab.guacamole.util.{ TestUtil, GuacFunSuite }
import org.scalatest.{ Matchers, FunSuite }
import org.bdgenomics.formats.avro.GenotypeAllele
import scala.collection.JavaConversions._
import org.hammerlab.guacamole.pileup.Pileup

class EvalCommandSuite extends GuacFunSuite with Matchers {

  val chrM = TestUtil.testDataPath("chrM.sorted.bam")

  def run(args: String*): Seq[String] = {
    EvalCommand.Caller.run(sc, args: _*).asInstanceOf[RDD[String]].collect()
  }
  def runOnExample(args: String*): Seq[String] = {
    val fullArgs: Seq[String] = Seq(chrM, "::") ++ args ++ Seq("--loci", "chrM:5000-5005")
    run(fullArgs: _*)
  }

  sparkTest("basic") {
    runOnExample("locus") should equal((5000 until 5005).map(_.toString))
    runOnExample("locus * 2") should equal(Seq("10000.0", "10002.0", "10004.0", "10006.0", "10008.0"))
  }

  sparkTest("includes") {
    runOnExample(
      "--include-code", "function locusTimesTwo() {return locus * 2;}",
      "--include-code", "function locusTimesNegativeTwo() {return locus * -2; }",
      "locusTimesNegativeTwo()") should equal(
        Seq("-10000.0", "-10002.0", "-10004.0", "-10006.0", "-10008.0"))

    runOnExample(
      "--include-code", "function locusTimesTwo() {return locus * 2;}",
      "--include-code", "function locusTimesNegativeTwo() {return locus * -2; }",
      "locusTimesNegativeTwo()",
      "locusTimesTwo()") should equal(
        Seq("-10000.0, 10000.0", "-10002.0, 10002.0", "-10004.0, 10004.0", "-10006.0, 10006.0", "-10008.0, 10008.0"))
  }

  sparkTest("pileups") {
    runOnExample("locus", "pileup().count('')", "pileup().count('element.isMatch()')") should equal(Seq(
      "5000, 161, 161", "5001, 159, 158", "5002, 155, 151", "5003, 156, 155", "5004, 157, 153"
    ))

  }

}
