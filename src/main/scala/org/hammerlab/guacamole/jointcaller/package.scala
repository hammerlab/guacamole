package org.hammerlab.guacamole

package object jointcaller {
  type AllelicDepths = Map[String, Int]

  object AllelicDepths {
    def apply(entries: (String, Int)*): AllelicDepths = entries.toMap
  }
}
