package org.hammerlab.guacamole.loci.set

trait Util {
  def makeLociSet(str: String, lengths: (String, Long)*): LociSet =
    LociParser(str).result(lengths.toMap)
}
