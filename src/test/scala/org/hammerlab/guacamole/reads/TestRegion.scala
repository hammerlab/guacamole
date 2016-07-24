package org.hammerlab.guacamole.reads

import org.hammerlab.guacamole.reference.{ContigName, ReferenceRegion}

case class TestRegion(contigName: ContigName, start: Long, end: Long) extends ReferenceRegion
