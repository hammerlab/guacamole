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

package org.bdgenomics.guacamole.callers

import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.callers.SomaticThresholdVariantCaller.Arguments
import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole.TestUtil

class SomaticThresholdVariantCallerSuite extends SparkFunSuite {
  val output = "/tmp/somatic.threshold.chr20.vcf"
  TestUtil.deleteIfExists(output)

  sparkTest("somatic threshold") {
    SomaticThresholdVariantCaller.runWrapper(
      sc,
      Args4j[Arguments](
        Array[String](
          "-tumor-reads", TestUtil.testDataPath("synth1.tumor.100k-200k.withmd.bam"),
          "-normal-reads", TestUtil.testDataPath("synth1.normal.100k-200k.withmd.bam"),
          "-parallelism", "20",
          "-loci", "20:100000-200000",
          "-out", output
        )
      )
    )
  }
}
