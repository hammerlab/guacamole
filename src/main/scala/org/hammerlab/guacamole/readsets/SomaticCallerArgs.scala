package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.Common.Arguments.GenotypeOutput
import org.hammerlab.guacamole.distributed.LociPartitionUtils

trait SomaticCallerArgs extends GenotypeOutput with TumorNormalReadsArgs with LociPartitionUtils.Arguments
