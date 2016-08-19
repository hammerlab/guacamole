package org.hammerlab.guacamole.util

object BasesUtil {
  implicit def basesToString = Bases.basesToString _
  implicit def stringToBases = Bases.stringToBases _
}
