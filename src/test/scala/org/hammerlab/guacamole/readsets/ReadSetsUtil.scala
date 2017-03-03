package org.hammerlab.guacamole.readsets

import org.apache.spark.SparkContext
import org.hammerlab.genomics.loci.parsing.ParsedLoci
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.test.ContigLengthsUtil
import org.hammerlab.guacamole.jointcaller.InputCollection
import org.hammerlab.guacamole.reads.ReadsUtil
import org.hammerlab.guacamole.readsets.io.TestInputConfig

trait ReadSetsUtil
  extends ContigLengthsUtil
    with ReadsUtil {

  def sc: SparkContext

  def makeReadSets(inputs: InputCollection, lociStr: String): (ReadSets, LociSet) =
    makeReadSets(inputs, ParsedLoci(lociStr))

  def makeReadSets(inputs: InputCollection, loci: ParsedLoci): (ReadSets, LociSet) = {
    val readsets = ReadSets(sc, inputs.items, config = TestInputConfig(loci))
    (readsets, LociSet(loci, readsets.contigLengths))
  }
}

object ReadSetsUtil {
  type TestRead = (String, Int, Int, Int)
  type TestReads = Seq[TestRead]
}
