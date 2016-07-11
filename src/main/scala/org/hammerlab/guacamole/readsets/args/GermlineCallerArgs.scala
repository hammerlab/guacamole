package org.hammerlab.guacamole.readsets.args

import org.hammerlab.guacamole.loci.partitioning.AllLociPartitionerArgs
import org.hammerlab.guacamole.variants.Concordance.ConcordanceArgs
import org.hammerlab.guacamole.variants.GenotypeOutputArgs

trait GermlineCallerArgs
  extends GenotypeOutputArgs
    with ReadsArgs
    with ConcordanceArgs
    with AllLociPartitionerArgs
