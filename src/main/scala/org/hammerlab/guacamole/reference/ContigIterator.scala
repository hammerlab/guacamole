package org.hammerlab.guacamole.reference

/**
 * Given some elements and a function for extracting a [[ContigName]] from them, return an iterator that is restricted
 * to the given contig.
 *
 * See companion object for public constructors which automatically infer [[contigNameFn]] from the input type.
 */
class ContigIterator[+T] private(val contigName: ContigName,
                                 elems: BufferedIterator[T],
                                 contigNameFn: T => ContigName)
  extends BufferedIterator[T] {

  override def head: T =
    if (hasNext)
      elems.head
    else
      throw new NoSuchElementException

  override def hasNext: Boolean = {
    elems.hasNext && contigNameFn(elems.head) == contigName
  }

  override def next(): T = {
    val n = head
    elems.next()
    n
  }
}

/**
 * Public [[ContigIterator]] constructors that infer the [[ContigName]]-function from the iterator-type T.
 */
object ContigIterator {
  def apply[T <: HasContig](elems: BufferedIterator[T]): ContigIterator[T] =
    new ContigIterator(elems.head.contigName, elems, (t: T) => t.contigName)

  def apply[T](elems: BufferedIterator[T], contigNameFn: T => ContigName): ContigIterator[T] =
    new ContigIterator(contigNameFn(elems.head), elems, contigNameFn)
}
