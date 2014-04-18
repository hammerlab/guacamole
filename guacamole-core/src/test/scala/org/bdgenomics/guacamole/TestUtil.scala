package org.bdgenomics.guacamole

/**
 * This is copied from SparkFunSuite in ADAM, which is for some reason not exposed.
 *
 */
import org.scalatest._
import java.net.ServerSocket
import org.apache.spark.SparkContext
import org.apache.log4j.{ Logger, Level }

object TestUtil {
  object SparkTest extends org.scalatest.Tag("org.bdgenomics.guacamole.SparkScalaTestFunSuite")

  object SparkLogUtil {
    /**
     * set all loggers to the given log level.  Returns a map of the value of every logger
     * @param level Log4j level
     * @param loggers Loggers to apply level to
     * @return
     */
    def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
      loggers.map {
        loggerName =>
          val logger = Logger.getLogger(loggerName)
          val prevLevel = logger.getLevel
          logger.setLevel(level)
          loggerName -> prevLevel
      }.toMap
    }

    /**
     * turn off most of spark logging.  Returns a map of the previous values so you can turn logging back to its
     * former values
     */
    def silenceSpark() = {
      setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
    }
  }

  trait SparkFunSuite extends FunSuite with BeforeAndAfter {

    val sparkPortProperty = "spark.driver.port"

    var sc: SparkContext = _
    var maybeLevels: Option[Map[String, Level]] = None

    def createSpark(sparkName: String, silenceSpark: Boolean = true): SparkContext = {
      // Use the same context properties as ADAM commands
      SerializationUtil.setupContextProperties()
      // Silence the Spark logs if requested
      maybeLevels = if (silenceSpark) Some(SparkLogUtil.silenceSpark()) else None
      synchronized {
        // Find an unused port
        val s = new ServerSocket(0)
        System.setProperty(sparkPortProperty, s.getLocalPort.toString)
        // Allow Spark to take the port we just discovered
        s.close()

        // Create a spark context
        new SparkContext("local[4]", sparkName)
      }
    }

    def destroySpark() {
      // Stop the context
      sc.stop()
      sc = null

      // See notes at:
      // http://blog.quantifind.com/posts/spark-unit-test/
      // That post calls for clearing 'spark.master.port', but this thread
      // https://groups.google.com/forum/#!topic/spark-users/MeVzgoJXm8I
      // suggests that the property was renamed 'spark.driver.port'
      System.clearProperty(sparkPortProperty)

      maybeLevels match {
        case None =>
        case Some(levels) =>
          for ((className, level) <- levels) {
            SparkLogUtil.setLogLevels(level, List(className))
          }
      }
    }

    def sparkBefore(beforeName: String, silenceSpark: Boolean = true)(body: => Unit) {
      before {
        sc = createSpark(beforeName, silenceSpark)
        try {
          // Run the before block
          body
        } finally {
          destroySpark()
        }
      }
    }

    def sparkAfter(beforeName: String, silenceSpark: Boolean = true)(body: => Unit) {
      after {
        sc = createSpark(beforeName, silenceSpark)
        try {
          // Run the after block
          body
        } finally {
          destroySpark()
        }
      }
    }

    def sparkTest(name: String, silenceSpark: Boolean = true)(body: => Unit) {
      test(name, SparkTest) {
        sc = createSpark(name, silenceSpark)
        try {
          // Run the test
          body
        } finally {
          destroySpark()
        }
      }
    }
  }
}
