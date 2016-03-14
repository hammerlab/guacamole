/*
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole.likelihood

import org.bdgenomics.adam.models.ReferencePosition
import org.hammerlab.guacamole.ReadsUtil
import org.hammerlab.guacamole.pileup.PileupElement
import org.hammerlab.guacamole.reads.MappedRead
import org.scalatest.FunSuite
import org.scalactic.TolerantNumerics

class MutectLikelihoodModelSuite extends FunSuite {

  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(1e-6f)

  //Demo data
  val all_c = for (readID <- 1 to 10) yield {
    val refRead = ReadsUtil.refRead(30 + (readID - 1))
    PileupElement(
      refRead,
      1,
      'C')
  }

  val some_muts = for (readID <- 1 to 10) yield {
    val read = if (readID <= 3) ReadsUtil.altRead(30 + (readID - 1)) else ReadsUtil.refRead(30 + (readID - 1))
    PileupElement(
      read,
      1,
      'C')
  }

  test("Likelihood of simple model, no errors or mutants, all reference sites") {
    val m0likelihood = M0Model.logLikelihood("C", "A", all_c, None)
    val mflikelihood = MfmModel.logLikelihood("C", "A", all_c, None)
    // Assuming f = 0, mflikelihood or m0likelihood
    // in R:
    // options(digits=10)
    // p = seq(30,39)
    // e = 10^(-p/10)
    // log10(prod(1-e))
    // > -0.001901013984
    assert(m0likelihood === -0.001901013984)
    assert(mflikelihood === m0likelihood)

    val mhlikelihood = MHModel.logLikelihood("C", "A", all_c, None)
    // assuming f = 0.5 mhlikelihood
    // in R
    // log10(prod(0.5*(e/3) + 0.5*(1-e)))
    // -3.01156717
    assert(mhlikelihood === -3.01156717)
  }
  test("Likelihood of simple model, all errors, no reference or mutant sites") {
    val m0likelihood = M0Model.logLikelihood("T", "G", all_c, None)
    val mflikelihood = MfmModel.logLikelihood("T", "G", all_c, None)
    val mhlikelihood = MHModel.logLikelihood("T", "G", all_c, None)

    // Assuming f = 0 or 0.5, mflikelihood or m0likelihood
    // in R:
    // options(digits=10)
    // p = seq(30,39)
    // e = 10^(-p/10)
    // log10(prod(e/3))
    // > -39.27121
    assert(m0likelihood === -39.27121255)
    assert(mflikelihood === m0likelihood)
    assert(mhlikelihood === m0likelihood)

  }

  test("Likelihood of simple model, all mutant, no error or reference sites") {
    val m0likelihood = M0Model.logLikelihood("A", "C", all_c, None)
    // Assuming f = 0, mflikelihood or m0likelihood
    // in R:
    // options(digits=10)
    // p = seq(30,39)
    // e = 10^(-p/10)
    // log10(prod(e/3))
    // > -39.27121
    assert(m0likelihood === -39.27121255)

    val mhlikelihood = MHModel.logLikelihood("C", "A", all_c, None)
    // assuming f = 0.5 mhlikelihood
    // in R
    // log10(prod(0.5*(1-e) + 0.5*(e/3)))
    // -3.01156717
    assert(mhlikelihood === -3.01156717)

  }

  test("Likelihood/log10 likelihood of small mutant, first 3 reads (30,31,32) MfModel") {
    val mflikelihood = MfmModel.logLikelihood("C", "A", some_muts, None)
    val m03likelihood = MfmModel.logLikelihood("C", "A", some_muts, Some(0.3))
    val mhlikelihood = MHModel.logLikelihood("C", "A", some_muts, None)
    val m0likelihood = M0Model.logLikelihood("C", "A", some_muts, None)
    // f should be 0.3
    // in R
    // pm = seq(30,32)
    // pr = seq(33,39)
    // em = 10^(-pm/10)
    // er = 10^(-pr/10)
    // log10(prod(0.3*(1-em) + 0.7*(em/3))*prod( 0.3*(er/3) + 0.7*(1-er)))
    // -2.653910268
    assert(mflikelihood === -2.653910268)
    assert(m03likelihood === mflikelihood) //same if f properly calculated here

    // the no mutants model in R
    // log10(prod(0*(1-em) + 1*(em/3))*prod( 0*(er/3) + 1*(1-er)))
    // -10.73221105
    assert(m0likelihood === -10.73221105)
    val logOddsMutant = MutectLogOdds.logOdds("C", "A", some_muts, None)
    assert(logOddsMutant === mflikelihood - m0likelihood)

    // the heterozygous site model in R:
    // log10(prod(0.5*(1-em) + 0.5*(em/3))*prod( 0.5*(er/3) + 0.5*(1-er)))
    // -3.01156717
    assert(mhlikelihood === -3.01156717)
    val logOddsHet = MutectSomaticLogOdds.logOdds("C", "A", some_muts, None)
    assert(logOddsHet === m0likelihood - mhlikelihood)
  }

}
