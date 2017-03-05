package org.hammerlab.guacamole

import org.hammerlab.genomics.bases.Bases

package object jointcaller {
  type AllelicDepths = Map[Bases, Int]

  object AllelicDepths {
    def apply(entries: (Bases, Int)*): AllelicDepths = entries.toMap
  }
}
