package org.hammerlab.guacamole.readsets.args

import org.hammerlab.guacamole.loci.partitioning.LociPartitionerArgs
import org.hammerlab.guacamole.variants.GenotypeOutputArgs

trait SomaticCallerArgs
  extends GenotypeOutputArgs
    with TumorNormalReadsArgs
