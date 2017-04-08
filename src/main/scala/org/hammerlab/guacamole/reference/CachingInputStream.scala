package org.hammerlab.guacamole.reference

import java.io.InputStream

case class CachingInputStream(is: InputStream)
  extends InputStream {
  override def read(): Int = ???
}
