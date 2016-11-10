package org.hammerlab.guacamole.readsets

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.jointcaller.InputCollection
import org.hammerlab.guacamole.loci.parsing.ParsedLoci
import org.hammerlab.guacamole.reads.ReadsUtil
import org.hammerlab.guacamole.readsets.io.{InputConfig, TestInputConfig}

trait ReadSetsUtil
  extends ContigLengthsUtil
    with ReadsUtil {

  def sc: SparkContext

  def makeReadSets(inputs: InputCollection, loci: ParsedLoci): ReadSets =
    ReadSets(sc, inputs.items, config = TestInputConfig(loci))
}

object ReadSetsUtil {
  type TestRead = (String, Int, Int, Int)
  type TestReads = Seq[TestRead]
}
