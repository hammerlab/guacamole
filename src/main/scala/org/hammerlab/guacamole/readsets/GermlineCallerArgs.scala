package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.Common.Arguments.GenotypeOutput
import org.hammerlab.guacamole.Concordance.ConcordanceArgs
import org.hammerlab.guacamole.distributed.LociPartitionUtils

trait GermlineCallerArgs extends GenotypeOutput with ReadsArgs with ConcordanceArgs with LociPartitionUtils.Arguments
