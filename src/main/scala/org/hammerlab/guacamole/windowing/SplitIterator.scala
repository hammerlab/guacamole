package org.hammerlab.guacamole.windowing

object SplitIterator {
  /**
   * Split an iterator of key value pairs into separate iterators, one for each key, without buffering more data into
   * memory than necessary. The keys must be integers between 0 and the provided upper bound.
   *
   * Given an iterator of (k, v) where 0 <= k < num and v is of arbitrary type, returns a sequence of length num. The
   * i'th element in the sequence is an iterator over the values v where k = i.
   *
   * @param num number of keys. Each key k must be 0 <= k < num.
   * @param iterator source iterator of (key, value) pairs. It will be consumed as the result iterators are read.
   * @tparam V
   * @return a buffered iterator for each key in 0 .. num.
   */
  def split[V](num: Int, iterator: Iterator[V], idxFn: V => Int): Vector[BufferedIterator[V]] = {
    val buffers = (0 until num).map(_ => new collection.mutable.Queue[V])
    (0 until num).toVector.map(index => new SplitIterator[V](index, buffers, iterator, idxFn))
  }
}

/**
 * An iterator over values that pulls from a common source (key, value) iterator and yields values associated with a
 * particular key. Buffers any elements that don't match the key we're interested in into a shared collection of queues.
 *
 * @param myIndex which key this is an iterator over
 * @param buffers queues to buffer elements
 * @param source source (key, value) iterator
 * @tparam V
 */
private class SplitIterator[V](myIndex: Int,
                               buffers: Seq[collection.mutable.Queue[V]],
                               source: Iterator[V],
                               idxFn: V => Int) extends BufferedIterator[V] {

  val myBuffer = buffers(myIndex)

  override def hasNext: Boolean = {
    while (myBuffer.isEmpty && source.hasNext) {
      advance()
    }
    myBuffer.nonEmpty
  }

  override def next(): V = {
    val result = head
    myBuffer.dequeue()
    result
  }

  override def head: V = {
    while (myBuffer.isEmpty) {
      advance() // may throw.
    }
    myBuffer.head
  }

  private def advance(): Unit = {
    val element = source.next() // may throw.
    buffers(idxFn(element)) += element
  }
}

