package org.hammerlab.guacamole.reference

import org.hammerlab.genomics.bases
import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.test.implicits

// TODO(ryan): fold into bases.BasesUtil
trait BasesUtil
  extends bases.BasesUtil
  with implicits.Templates {
  implicit def baseToByte(base: Base): Byte = base.byte
  implicit val convertBase = convertOpt[Base, Byte] _
  implicit val convertSeqBase = toVector[Base, Bases] _
  implicit val convertSeqString = toVector[String, Bases] _
  implicit val convertBaseTuple2 = convertTuple2[Base, Base, Bases, Bases] _
  implicit def convertBaseTuple[T] = convertKey[Base, T, Bases] _
  implicit def convertStringTuple[T] = convertKey[String, T, Bases] _
}
