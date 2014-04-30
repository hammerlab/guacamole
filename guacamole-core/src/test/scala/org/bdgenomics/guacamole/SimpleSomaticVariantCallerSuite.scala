
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

import org.bdgenomics.adam.avro.ADAMRecord
import org.scalatest.matchers.ShouldMatchers
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.callers.SimpleSomaticVariantCaller

class SimpleSomaticVariantCallerSuite extends TestUtil.SparkFunSuite with ShouldMatchers {

  def loadReads(filename: String, locus: Long = 0): RDD[ADAMRecord] = {
    /* grab the path to the SAM file we've stashed in the resources subdirectory */
    val path = ClassLoader.getSystemClassLoader.getResource(filename).getFile
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    sc.adamLoad(path)
  }

  sparkTest("No variants when tumor/normal identical") {
    val reads = loadReads("same_start_reads.sam", 0)
    val genotypes = SimpleSomaticVariantCaller.callVariants(reads, reads)
    genotypes.collect.length should be(0)
  }

  sparkTest("Simple SNV in same_start_reads") {
    val normal = loadReads("same_start_reads.sam", 0)
    val tumor = loadReads("same_start_reads_snv_tumor.sam", 0)
    val genotypes = SimpleSomaticVariantCaller.callVariants(normal, tumor)
    genotypes.collect.length should be(1)
  }

}

