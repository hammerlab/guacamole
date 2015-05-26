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

import org.bdgenomics.adam.util.PhredUtils
import org.hammerlab.guacamole.likelihood.LikelihoodSuite
import org.hammerlab.guacamole.util.TestUtil
import org.hammerlab.guacamole.variants.{ Allele, Genotype }

/**
 * Some utility functions for [[LikelihoodSuite]].
 */
object ReadsUtil {

  val referenceBase = 'C'.toByte

  def makeGenotype(alleles: String*): Genotype = {
    // If we later change Genotype to work with Array[byte] instead of strings, we can use this function to convert
    // to byte arrays.
    Genotype(alleles.map(allele => Allele(Seq(referenceBase), Bases.stringToBases(allele))): _*)
  }

  def makeGenotype(alleles: (Char, Char)): Genotype = {
    makeGenotype(alleles.productIterator.map(_.toString).toList: _*)
  }

  val errorPhred30 = PhredUtils.phredToErrorProbability(30)
  val errorPhred40 = PhredUtils.phredToErrorProbability(40)

  def refRead(phred: Int) = TestUtil.makeRead("C", "1M", "1", 1, "chr1", Some(Array(phred)))
  def altRead(phred: Int) = TestUtil.makeRead("A", "1M", "0C0", 1, "chr1", Some(Array(phred)))

}
