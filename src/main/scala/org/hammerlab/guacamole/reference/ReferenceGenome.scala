package org.hammerlab.guacamole.reference


trait ReferenceGenome {

  def getContig(contigName: String): Array[Byte]

}
