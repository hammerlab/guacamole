package org.hammerlab.guacamole.readsets.args

import org.hammerlab.commands.Args
import org.hammerlab.guacamole.variants.GenotypeOutputArgs

trait GermlineCallerArgs
  extends Args
    with GenotypeOutputArgs
    with SingleSampleArgs
