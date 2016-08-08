package org.hammerlab.guacamole.readsets.args

import org.kohsuke.args4j.{Argument, Option => Args4JOption}
import org.kohsuke.args4j.spi.StringArrayOptionHandler

/**
 * Common command-line arguments for loading in one or more sets of reads, and associating a sample-name with each.
 */
trait Arguments extends Base {

  @Argument(required = true, multiValued = true, usage = "FILE1 FILE2 FILE3")
  var paths: Array[String] = Array()

  @Args4JOption(name = "--sample-names", handler = classOf[StringArrayOptionHandler], usage = "name1 ... nameN")
  var sampleNames: Array[String] = Array()
}

