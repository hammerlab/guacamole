package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.distributed.LociPartitionUtils
import org.hammerlab.guacamole.variants.Concordance.ConcordanceArgs
import org.hammerlab.guacamole.variants.GenotypeOutputArgs

trait GermlineCallerArgs
  extends GenotypeOutputArgs
    with ReadsArgs
    with ConcordanceArgs
    with LociPartitionUtils.Arguments
