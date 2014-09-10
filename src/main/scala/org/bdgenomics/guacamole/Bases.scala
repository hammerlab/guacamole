package org.bdgenomics.guacamole

/**
 * We represent a nucleotide base as a Byte, whose value is equal to the ASCII value of the character representing the
 * base (e.g. A, C, T, G). We represent a nucleotide sequence as an Array[Byte].
 *
 * This is an optimization over java Chars and Strings which use two bytes per character.
 *
 * Unfortunately, working with Bytes and especially Array[Byte] is a bit of a hassle and can be error-prone. The
 * functions here are for working with these types correctly.
 *
 * Things to know:
 *
 *  - you can compare two bases b1, b2 as you'd expect: b1 == b2
 *
 *  - you CANNOT compare two Array[Byte] instances with == (or even .equals). Use the equal() function defined here.
 *
 *  - If b is a base (say "A") then b.toString does NOT give you want (in this case it would give you "65"). Use the
 *    baseToString() function defined here.
 *
 */
object Bases {

  /** Standard bases. Note that other bases are sometimes used as well (e.g. "N"). */
  val A = "A".getBytes()(0)
  val C = "C".getBytes()(0)
  val T = "T".getBytes()(0)
  val G = "G".getBytes()(0)

  /** Is the given base one of the 4 canonical DNA bases? */
  def isStandardBase(base: Byte): Boolean = {
    base == Bases.A || base == Bases.C || base == Bases.T || base == Bases.G
  }

  /** Throw an error if the given base is not one of the canonical DNA bases. */
  def assertStandardBase(base: Byte) = {
    assert(isStandardBase(base), "Invalid base: %s".format(base.toChar.toString))
  }

  /** Are all the given bases standard? */
  def allStandardBases(bases: Seq[Byte]) = {
    assert(bases.forall(b => isStandardBase(b)), "Invalid base array: %s".format(bases.map(_.toChar).mkString))
  }

  /** Throw an error if any of the given bases are not standard. */
  def assertAllStandardBases(bases: Seq[Byte]) = {
    assert(bases.forall(b => isStandardBase(b)), "Invalid base array: %s".format(bases.map(_.toChar).mkString))
  }

  /** Convert a string (e.g. "AAAGGC") to a byte array. */
  def stringToBases(string: String): Seq[Byte] = {
    string.toUpperCase.getBytes
  }

  /** Convert a base to a 1-character string. */
  def baseToString(base: Byte): String = {
    base.toChar.toString
  }

  /** Convert a base sequence to a String. */
  def basesToString(bases: Iterable[Byte]): String = {
    bases.map(_.toChar).mkString
  }

}
