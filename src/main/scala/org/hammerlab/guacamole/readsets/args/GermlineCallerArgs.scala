package org.hammerlab.guacamole.readsets.args

import org.hammerlab.cli.args4j.Args
import org.hammerlab.genomics.readsets.args.impl.{ PathPrefixArg, PrefixedSingleSampleArgs }
import org.hammerlab.guacamole.variants.GenotypeOutputArgs

class GermlineCallerArgs
  extends Args
    with PrefixedSingleSampleArgs
    with GenotypeOutputArgs
    with PathPrefixArg
