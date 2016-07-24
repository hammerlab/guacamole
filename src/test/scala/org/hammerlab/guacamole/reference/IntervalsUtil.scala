package org.hammerlab.guacamole.reference

trait IntervalsUtil {
  def makeIntervals(intervals: Seq[(Int, Int, Int)]): BufferedIterator[TestInterval] =
    (for {
      (start, end, num) <- intervals
      i <- 0 until num
    } yield
      TestInterval(start, end)
    ).iterator.buffered
}
