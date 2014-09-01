package org.bdgenomics.guacamole.reads

case class MateProperties(isFirstInPair: Boolean,
                          inferredInsertSize: Option[Int],
                          isMateMapped: Boolean,
                          mateReferenceContig: Option[String],
                          mateStart: Option[Long],
                          isMatePositiveStrand: Boolean) {
  // If the mate is mapped check that we also know where it is mapped
  assert(!isMateMapped || (mateReferenceContig.isDefined && mateStart.isDefined))
}

