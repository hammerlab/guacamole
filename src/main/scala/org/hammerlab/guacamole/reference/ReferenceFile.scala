package org.hammerlab.guacamole.reference

import grizzled.slf4j.Logging
import org.hammerlab.genomics.reference.{ ContigName, ContigSequence }
import org.hammerlab.guacamole.reference.FastaIndex.Entry
import org.hammerlab.paths.Path

case class ReferenceFile(path: Path, entries: Map[ContigName, Entry])
  extends ReferenceGenome
    with Logging {

  override def source: Option[Path] = Some(path)

  @transient lazy val contigs = collection.concurrent.TrieMap[ContigName, Contig]()

  override def apply(contigName: ContigName): ContigSequence =
    contigs.getOrElseUpdate(
      contigName,
      Contig(contigName, entries(contigName), path)
    )
}

object ReferenceFile {
  def apply(path: Path): ReferenceFile =
    ReferenceFile(
      path,
      FastaIndex(Path(path.uri.toString + ".fai")).entries
    )
}
