package org.hammerlab.guacamole.strings

trait TruncatedToString {
  override def toString: String = truncatedString(Int.MaxValue)

  /** String representation, truncated to maxLength characters. */
  def truncatedString(maxLength: Int = 500): String = {
    TruncatedToString(stringPieces, maxLength)
  }

  /**
   * Iterator over string representations of
   */
  def stringPieces: Iterator[String]
}

object TruncatedToString {
  /**
   * Like Scala's List.mkString method, but supports truncation.
   *
   * Return the concatenation of an iterator over strings, separated by separator, truncated to at most maxLength
   * characters. If truncation occurs, the string is terminated with ellipses.
   */
  def apply(pieces: Iterator[String],
            maxLength: Int,
            separator: String = ",",
            ellipses: String = " [...]"): String = {
    val builder = StringBuilder.newBuilder
    var remaining: Int = maxLength
    while (pieces.hasNext && remaining > ellipses.length) {
      val string = pieces.next()
      builder.append(string)
      if (pieces.hasNext) builder.append(separator)
      remaining -= string.length + separator.length
    }
    if (pieces.hasNext) builder.append(ellipses)
    builder.result
  }
}
