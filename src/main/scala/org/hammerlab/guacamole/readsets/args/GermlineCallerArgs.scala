package org.hammerlab.guacamole.readsets.args

import org.hammerlab.guacamole.commands.Args
import org.hammerlab.guacamole.variants.Concordance.ConcordanceArgs
import org.hammerlab.guacamole.variants.GenotypeOutputArgs

trait GermlineCallerArgs
  extends Args
    with GenotypeOutputArgs
    with SingleSampleArgs
    with ConcordanceArgs
