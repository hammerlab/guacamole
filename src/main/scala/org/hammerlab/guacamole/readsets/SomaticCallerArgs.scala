package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.distributed.LociPartitionUtils
import org.hammerlab.guacamole.variants.GenotypeOutputArgs

trait SomaticCallerArgs
  extends GenotypeOutputArgs
    with TumorNormalReadsArgs
    with LociPartitionUtils.Arguments
