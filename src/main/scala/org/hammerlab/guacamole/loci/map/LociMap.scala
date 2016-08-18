package org.hammerlab.guacamole.loci.map

import java.io.{OutputStream, PrintStream}

import org.hammerlab.guacamole.loci.set.{LociSet, Builder => LociSetBuilder}
import org.hammerlab.guacamole.reference.{ContigName, ReferenceRegion}
import org.hammerlab.guacamole.strings.TruncatedToString

import scala.collection.immutable.TreeMap
import scala.collection.{SortedMap, mutable}

/**
 * An immutable map from loci to a instances of an arbitrary type T.
 *
 * Since contiguous genomic intervals are a common case, this is implemented with sets of (start, end) intervals.
 *
 * All intervals are half open: inclusive on start, exclusive on end.
 *
 * @param map Map from contig names to [[Contig]] instances giving the regions and values on that contig.
 */
case class LociMap[T](@transient private val map: SortedMap[ContigName, Contig[T]])
  extends TruncatedToString {

  /** The contigs included in this LociMap with a nonempty set of loci. */
  @transient lazy val contigs = map.values.toSeq

  /** The number of loci in this LociMap. */
  @transient lazy val count: Long = contigs.map(_.count).sum

  /** The "inverse map", i.e. a T -> LociSet map that gives the loci that map to each value. */
  @transient lazy val inverse: Map[T, LociSet] = {
    val mapOfBuilders = new mutable.HashMap[T, LociSetBuilder]()
    for {
      contig <- contigs
      (value, setContig) <- contig.inverse
    } {
      mapOfBuilders
        .getOrElseUpdate(value, new LociSetBuilder)
        .add(setContig)
    }
    mapOfBuilders.mapValues(_.result).toMap
  }

  /**
   * Return values corresponding to any ranges that overlap the given [[ReferenceRegion]], with a `halfWindowSize`
   * margin of error.
   */
  def getAll(r: ReferenceRegion, halfWindowSize: Int = 0): Set[T] =
    onContig(r.contigName).getAll(r.start - halfWindowSize, r.end + halfWindowSize)

  /**
   * Returns the loci map on the specified contig.
   *
   * @param contig The contig name
   * @return A [[Contig]] instance giving the loci mapping on the specified contig.
   */
  def onContig(contig: String): Contig[T] = map.getOrElse(contig, Contig[T](contig))

  /** Build a truncate-able toString() out of underlying contig pieces. */
  def stringPieces: Iterator[String] = contigs.iterator.flatMap(_.stringPieces)

  def prettyPrint(os: OutputStream): Unit = {
    val ps =
      os match {
        case ps: PrintStream => ps
        case _ => new PrintStream(os)
      }

    stringPieces.foreach(ps.println)
  }
}

object LociMap {
  /** Returns a new Builder instance for constructing a LociMap. */
  def newBuilder[T]: Builder[T] = new Builder[T]()

  /** Construct an empty LociMap. */
  def apply[T](): LociMap[T] = LociMap(TreeMap[ContigName, Contig[T]]())

  /** The following convenience constructors are only called by Builder. */
  private[map] def apply[T](contigs: (String, Long, Long, T)*): LociMap[T] = {
    val builder = new Builder[T]
    for {
      (contig, start, end, value) <- contigs
    } {
      builder.put(contig, start, end, value)
    }
    builder.result()
  }

  private[map] def fromContigs[T](contigs: Iterable[Contig[T]]): LociMap[T] =
    LociMap(
      TreeMap(
        contigs.map(contig => contig.name -> contig).toSeq: _*
      )
    )
}
