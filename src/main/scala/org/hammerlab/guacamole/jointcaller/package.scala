package org.hammerlab.guacamole

import org.hammerlab.genomics.bases.Bases
import org.hammerlab.genomics.readsets.PerSample

package object jointcaller {

  type Samples = SamplesI[Sample]

  implicit class AllelicDepths(val map: Map[Bases, Int]) extends AnyVal {
    def take(num: Int): AllelicDepths =
      map
        .toVector
        .sortBy(-_._2)
        .take(num)
        .toMap

    override def toString: String = s"AD(${map.map { case (allele, depth) ⇒ s"$allele → $depth" }.mkString(",")})"
  }

  object AllelicDepths {
    implicit def unwrapAllelicDepths(allelicDepths: AllelicDepths): Map[Bases, Int] = allelicDepths.map
    def apply(entries: (Bases, Int)*): AllelicDepths = entries.toMap
  }
}
