package org.hammerlab.guacamole.reference

import org.hammerlab.genomics.reference.ContigName
import org.hammerlab.guacamole.reference.FastaIndex.Entry
import org.hammerlab.paths.Path

import scala.collection.concurrent

case class ReferenceFile(path: Path, entries: Map[ContigName, Entry])
  extends ReferenceGenome {

  override def source: Option[Path] = Some(path)

  @transient lazy val contigs = concurrent.TrieMap[ContigName, FileContig]()

  override def apply(contigName: ContigName): FileContig =
    contigs.getOrElseUpdate(
      contigName,
      entries.get(contigName) match {
        case Some(entry) ⇒
          FileContig(contigName, entries(contigName), path)
        case None ⇒
          throw ContigNotFound(contigName, entries.keys)
      }

    )
}

object ReferenceFile {
  def apply(path: Path): ReferenceFile =
    ReferenceFile(
      path,
      FastaIndex(Path(path.toString + ".fai")).entries
    )
}
