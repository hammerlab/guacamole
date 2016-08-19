package org.hammerlab.guacamole.readsets

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.jointcaller.InputCollection
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.reads.ReadsUtil
import org.hammerlab.guacamole.readsets.io.InputFilters

trait ReadSetsUtil
  extends ContigLengthsUtil
    with ReadsUtil {

  def sc: SparkContext

  def makeReadSets(inputs: InputCollection, loci: LociParser): ReadSets =
    ReadSets(sc, inputs.items, filters = InputFilters(overlapsLoci = loci))
}

object ReadSetsUtil {
  type TestRead = (String, Int, Int, Int)
  type TestReads = Seq[TestRead]
}
