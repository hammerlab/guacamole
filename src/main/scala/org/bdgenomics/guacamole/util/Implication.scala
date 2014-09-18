package org.bdgenomics.guacamole.util

final class Implication[A](val _value: A) extends AnyVal {
  def ==>[B](other: => B)(implicit evA: A <:< Boolean, evB: B <:< Boolean): Boolean = !evA(_value) || evB(other)
}

object Implication {
  implicit def wrapImplication[A](anything: A): Implication[A] = new Implication(anything)
  implicit def unwrapImplication[A](id: Implication[A]): A = id._value
}
