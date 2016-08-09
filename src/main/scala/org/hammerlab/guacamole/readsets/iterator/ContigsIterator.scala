package org.hammerlab.guacamole.readsets.iterator

import org.apache.spark.Logging
import org.hammerlab.guacamole.reference.{ContigIterator, ContigName, HasContig}
import org.hammerlab.magic.iterator.SimpleBufferedIterator

import scala.collection.mutable

/**
 * Divide an iterator into a series of contig-restricted iterators.
 *
 * Takes an iterator of arbitrary elements and a function for extracting a [[ContigName]] from them.
 *
 * Emits [[(ContigName, ContigIterator)]] tuples.
 *
 * See companion object for public constructors.
 */
class ContigsIterator[T] private(it: BufferedIterator[T],
                                 contigNameFn: T => ContigName,
                                 throwOnContigRepeat: Boolean = true)
  extends SimpleBufferedIterator[(ContigName, ContigIterator[T])]
    with Logging {

  var contigRegions: ContigIterator[T] = _

  val seenContigs = mutable.Set[String]()

  override def _advance: Option[(ContigName, ContigIterator[T])] = {
    // Discard any remaining reads from the previous partition.
    if (contigRegions != null) {
      while (contigRegions.hasNext) {
        contigRegions.next()
      }
    }

    if (!it.hasNext)
      None
    else {
      contigRegions = ContigIterator(it, contigNameFn)
      val contigName = contigRegions.contigName

      if (seenContigs(contigName))
        if (throwOnContigRepeat)
          throw RepeatedContigException(
            s"Repeating $contigName after seeing contigs: ${seenContigs.mkString(",")}"
          )
        else
          log.warn(s"Repeating contig: $contigName")
      else
        seenContigs += contigName

      Some(contigNameFn(it.head) -> contigRegions)
    }
  }
}

/**
 * Public [[ContigsIterator]] constructors infer the [[ContigName]]-function from a [[HasContig]] or a
 * [[(HasContig, T)]].
 */
object ContigsIterator {

  def apply[T <: HasContig](it: BufferedIterator[T],
                            requireRegionsGroupedByContig: Boolean = true): ContigsIterator[T] =
    new ContigsIterator(it, _.contigName, requireRegionsGroupedByContig)

  def byKey[C <: HasContig, T](it: BufferedIterator[(C, T)],
                               requireRegionsGroupedByContig: Boolean = true): ContigsIterator[(C, T)] =
    new ContigsIterator(it, _._1.contigName)
}

case class RepeatedContigException(msg: String) extends Exception(msg)
