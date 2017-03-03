package org.hammerlab.guacamole.loci

import org.hammerlab.genomics.reference
import org.hammerlab.genomics.reference.Locus
import org.hammerlab.test.implicits

trait LocusUtil
  extends reference.test.LocusUtil {
  self: implicits.Templates ⇒
  implicit val toLociVector = toVector[Int, Locus] _
  implicit val toLociOption = convertOpt[Int, Locus] _
  implicit val toLociOptionVector = toVector[Option[Int], Option[Locus]] _
}
