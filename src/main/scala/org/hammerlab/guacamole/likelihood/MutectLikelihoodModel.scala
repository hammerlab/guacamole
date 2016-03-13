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

import org.bdgenomics.adam.util.PhredUtils.phredToErrorProbability
import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.pileup.PileupElement

import scala.math._

trait LikelihoodModel extends Serializable {
  def logLikelihood(ref: String,
                    alt: String,
                    obs: Seq[PileupElement],
                    f: Option[Double]): Double
}

case class LogOdds(m1: LikelihoodModel, m2: LikelihoodModel) {

  def logOdds(ref: String, alt: String,
              obs: Seq[PileupElement]): Double =
    m1.logLikelihood(ref, alt, obs, None) - m2.logLikelihood(ref, alt, obs, None)
}

case class ContamLogOdds(val m1: LikelihoodModel, val m2: LikelihoodModel) {

  def logOdds(ref: String, alt: String,
              obs: Seq[PileupElement],
              contam: Double): Double =
    m1.logLikelihood(ref, alt, obs, None) - m2.logLikelihood(ref, alt, obs, Some(contam))
}

object MutectLogOdds extends LogOdds(MfmModel, M0Model) {
}

object MutectContamLogOdds extends ContamLogOdds(MfmModel, MfmModel)

/**
 * Use for the log odds that a normal is not a heterozygous site
 */
object MutectSomaticLogOdds extends LogOdds(M0Model, MHModel) {
}

object M0Model extends LikelihoodModel {

  def logLikelihood(ref: String,
                    alt: String,
                    obs: Seq[PileupElement],
                    f: Option[Double]): Double =
    MfmModel.logLikelihood(ref, alt, obs, Some(0.0))
}

/**
 * M_{m, 0.5} -- probability of a heterozygous site
 */
object MHModel extends LikelihoodModel {

  def logLikelihood(ref: String,
                    alt: String,
                    obs: Seq[PileupElement],
                    f: Option[Double]): Double =
    MfmModel.logLikelihood(ref, alt, obs, Some(0.5))
}

/**
 * M_{m, f}
 */
object MfmModel extends LikelihoodModel {

  def P_bi(obs: PileupElement, r: String, m: String, f: Double): Double = {
    val ei = phredToErrorProbability(obs.qualityScore)
    val obsBases = Bases.basesToString(obs.sequencedBases)
    if (obsBases == r) {
      f * (ei / 3.0) + (1.0 - f) * (1.0 - ei)
    } else if (obsBases == m) {
      f * (1.0 - ei) + (1.0 - f) * (ei / 3.0)
    } else {
      ei / 3.0
    }
  }

  def logLikelihood(ref: String, alt: String,
                    obs: Seq[PileupElement],
                    f: Option[Double]): Double = {
    val fEstimate: Double = f.getOrElse(obs.count(p => Bases.basesToString(p.sequencedBases) == alt).toDouble / obs.size)
    obs.map { ob => log10(P_bi(ob, ref, alt, fEstimate)) }.sum
  }
}

