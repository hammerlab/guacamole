package org.hammerlab.guacamole.loci.set

trait LociSetUtils {
  def makeLociSet(str: String, lengths: (String, Long)*): LociSet =
    LociParser(str).result(lengths.toMap)
}
