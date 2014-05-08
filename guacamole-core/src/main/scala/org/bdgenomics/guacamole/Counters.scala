package org.bdgenomics.guacamole

import org.apache.spark.{ SparkContext, Accumulator }
import org.apache.spark.SparkContext._
import scala.collection.mutable
import org.bdgenomics.guacamole.Counters.{ Message, Counter, Event }

/**
 *  It's often useful to output counts of certain events that occur in RDD operations. The functions that operate on
 *  RDDs, however, can't print these counters since the transformations they apply to the RDDs may not execute until
 *  much later (Spark RDD transformations are lazy). This class provides a simple mechanism to register a counter to be
 *  outputted at program termination, after all RDD transformations have been forced. *
 *
 * @param sc spark context
 */
class Counters(sc: SparkContext) {
  private val events = new mutable.ArrayBuffer[Event]()
  private def addEvent(event: Event): Unit = events.append(event)
  private var maxCounterNameLength = 1

  /**
   * Add a counter with the given name.
   * @param name name of the counter, to be used when output is printed.
   * @return a spark accumulator for the counter
   */
  def counter(name: String): Accumulator[Long] = {
    maxCounterNameLength = math.max(name.length, maxCounterNameLength)
    val accumulator = sc.accumulator(0L)
    addEvent(Counter(name, accumulator))
    accumulator
  }

  /**
   * Since some functions that create counters may be called multiple times during a Guacamole execution, it can be
   * confusing to see a bunch of identically named counters printed at program termination. This function makes it
   * possible to print a message indicating, for example, which stage the subsequent counters belong to.
   */
  def log(message: String): Unit = {
    addEvent(Message(message))
  }

  /**
   * Print to stdout all the registered counters and log messages, in the order they were registered. Call this function
   * at the end of the process.
   */
  def print(): Unit = {
    val formatString = "\t%" + maxCounterNameLength.toString + "s: %,d"
    println("*** Counters ***")
    for (event <- events) event match {
      case Message(value)       => println("\n%s".format(value))
      case Counter(name, value) => println(formatString.format(name, value.value))
    }
  }
}
object Counters {
  abstract class Event
  private case class Message(value: String) extends Event
  private case class Counter(name: String, value: Accumulator[Long]) extends Event

  /** Create a new Counter instance */
  def apply(sc: SparkContext): Counters = new Counters(sc)

  /**
   * To avoid passing a Counter to every function that may need one, we provide a singleton default instance for
   * convenience. You must call initDefault() first before using it.
   */
  private var defaultInstance: Option[Counters] = None
  def initDefault(sc: SparkContext): Unit = {
    assert(defaultInstance.isEmpty, "Called initDefault twice.")
    defaultInstance = Some(Counters(sc))
  }

  /**
   * Get the default Counter instance.
   */
  def default(): Counters = defaultInstance match {
    case Some(counters) => counters
    case None           => throw new AssertionError("Call Counters.init first.")
  }
}
