package org.hammerlab.guacamole.reference

import org.hammerlab.genomics.reference.ContigName
import org.hammerlab.guacamole.reference.FastaIndex.Entry
import org.hammerlab.paths.Path

import scala.collection.concurrent

case class FileViewReference(path: Path, entries: Map[ContigName, Entry])
  extends ReferenceGenome {

  override def source: Option[Path] = Some(path)

  @transient lazy val contigs = concurrent.TrieMap[ContigName, FileViewContig]()

  override def apply(contigName: ContigName): FileViewContig =
    contigs.getOrElseUpdate(
      contigName,
      entries.get(contigName) match {
        case Some(entry) ⇒
          FileViewContig(contigName, entry, path)
        case None ⇒
          throw ContigNotFound(contigName, entries.keys)
      }
    )
}

object FileViewReference {
  def apply(path: Path): FileViewReference =
    FileViewReference(
      path,
      FastaIndex(Path(path + ".fai")).entries
    )
}
