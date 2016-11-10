package org.hammerlab.guacamole.util

import org.kohsuke.args4j.{CmdLineParser, OptionDef}
import org.kohsuke.args4j.spi.{OptionHandler, Parameters, Setter}

/**
 * Args4J option handler that populates an [[Option[Int]]].
 */
class IntOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Int]])
  extends OptionHandler[Option[Int]](parser, option, setter) {

  override def getDefaultMetaVariable: String = "path"

  override def parseArguments(params: Parameters): Int = {
    setter.addValue(Some(params.getParameter(0).toInt))
    1
  }
}
