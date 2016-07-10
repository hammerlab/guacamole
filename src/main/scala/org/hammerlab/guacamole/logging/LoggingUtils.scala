package org.hammerlab.guacamole.logging

object LoggingUtils {

  /** Time in milliseconds of last progress message. */
  var lastProgressTime: Long = 0

  /**
   * Print or log a progress message. For now, we just print to standard out, since ADAM's logging setup makes it
   * difficult to see log messages at the INFO level without flooding ourselves with parquet messages.
   * @param messages Strings to print or log, one per line.
   */
  def progress(messages: String*): Unit = {
    val current = System.currentTimeMillis
    val time =
      if (lastProgressTime == 0)
        java.util.Calendar.getInstance.getTime.toString
      else
        "%.2f sec. later".format((current - lastProgressTime) / 1000.0)

    println("--> [%15s]: %s".format(time, messages.mkString("\n")))
    System.out.flush()
    lastProgressTime = current
  }
}
