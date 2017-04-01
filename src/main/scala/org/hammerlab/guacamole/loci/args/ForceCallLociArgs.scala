package org.hammerlab.guacamole.loci.args

import java.nio.file.Path

import org.hammerlab.args4s.{ PathOptionHandler, StringOptionHandler }
import org.kohsuke.args4j.{ Option ⇒ Args4jOption }

trait ForceCallLociArgs {
  @Args4jOption(
    name = "--force-call-loci",
    usage = "Always call the given sites",
    handler = classOf[StringOptionHandler]
  )
  var forceCallLociStrOpt: Option[String] = None

  @Args4jOption(
    name = "--force-call-loci-file",
    usage = "Always call the given sites",
    handler = classOf[PathOptionHandler]
  )
  var forceCallLociFileOpt: Option[Path] = None
}
