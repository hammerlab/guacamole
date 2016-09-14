package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.bdgenomics.utils.cli.Args4jBase

trait Args extends Args4jBase {
  def validate(sc: SparkContext): Unit = {}
}
