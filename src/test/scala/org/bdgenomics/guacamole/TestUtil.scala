package org.bdgenomics.guacamole

/**
 * This is copied from SparkFunSuite in ADAM, which is for some reason not exposed.
 *
 */

import java.io.{ File, FileNotFoundException }
import java.net.ServerSocket

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.{ IKryoRegistrar, KryoInstantiator, KryoPool }
import org.apache.commons.io.FileUtils
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.bdgenomics.guacamole.pileup.Pileup
import org.bdgenomics.guacamole.reads.{ MappedRead, MateProperties, Read }
import org.scalatest._

import scala.math._

object TestUtil extends Matchers {

  object Implicits {
    implicit def basesToString = Bases.basesToString _
    implicit def stringToBases = Bases.stringToBases _
  }

  // As a hack to run a single unit test, you can set this to the name of a test to run only it. See the top of
  // DistributedUtilSuite for an example.
  var runOnly: String = ""

  // Serialization helper functions.
  lazy val kryoPool = {
    val instantiator = new KryoInstantiator().setRegistrationRequired(true).withRegistrar(new IKryoRegistrar {
      override def apply(kryo: Kryo): Unit = new GuacamoleKryoRegistrator().registerClasses(kryo)
    })
    KryoPool.withByteArrayOutputStream(1, instantiator)
  }
  def serialize(item: Any): Array[Byte] = {
    kryoPool.toBytesWithClass(item)
  }
  def deserialize[T](bytes: Array[Byte]): T = {
    kryoPool.fromBytes(bytes).asInstanceOf[T]
  }
  def testSerialization[T](item: T): Unit = {
    val serialized = serialize(item)
    val deserialized = deserialize[T](serialized)
    deserialized should equal(item)
  }

  def makeRead(sequence: String,
               cigar: String,
               mdtag: String,
               start: Long = 1,
               chr: String = "chr1",
               qualityScores: Option[Seq[Int]] = None,
               alignmentQuality: Int = 30): MappedRead = {

    val qualityScoreString = if (qualityScores.isDefined) {
      qualityScores.get.map(q => q + 33).map(_.toChar).mkString
    } else {
      sequence.map(x => '@').mkString
    }

    Read(
      sequence,
      cigarString = cigar,
      mdTagString = mdtag,
      start = start,
      referenceContig = chr,
      baseQualities = qualityScoreString,
      alignmentQuality = alignmentQuality
    ).getMappedReadOpt.get
  }

  def makePairedRead(
    chr: String = "chr1",
    start: Long = 1,
    alignmentQuality: Int = 30,
    isPositiveStrand: Boolean = true,
    isMateMapped: Boolean = false,
    mateReferenceContig: Option[String] = None,
    mateStart: Option[Long] = None,
    isMatePositiveStrand: Boolean = false,
    sequence: String = "ACTGACTGACTG",
    cigar: String = "12M",
    mdTag: String = "12"): MappedRead = {

    val qualityScoreString = sequence.map(x => '@').mkString

    Read(
      sequence,
      cigarString = cigar,
      start = start,
      referenceContig = chr,
      mdTagString = mdTag,
      isPositiveStrand = isPositiveStrand,
      baseQualities = qualityScoreString,
      alignmentQuality = alignmentQuality,
      matePropertiesOpt = Some(
        MateProperties(
          isFirstInPair = false,
          inferredInsertSize = None,
          isMateMapped = isMateMapped,
          mateReferenceContig = mateReferenceContig,
          mateStart = mateStart,
          isMatePositiveStrand = isMatePositiveStrand
        )
      )
    ).getMappedReadOpt.get
  }

  def assertBases(bases1: Iterable[Byte], bases2: String) = Bases.basesToString(bases1) should equal(bases2)

  def testDataPath(filename: String): String = {
    val resource = ClassLoader.getSystemClassLoader.getResource(filename)
    if (resource == null) throw new RuntimeException("No such test data file: %s".format(filename))
    resource.getFile
  }

  def loadReads(sc: SparkContext, filename: String): ReadSet = {
    /* grab the path to the SAM file we've stashed in the resources subdirectory */
    val path = testDataPath(filename)
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    ReadSet(sc, path)
  }

  def loadTumorNormalReads(sc: SparkContext,
                           tumorFile: String,
                           normalFile: String): (Seq[MappedRead], Seq[MappedRead]) = {
    val filters = Read.InputFilters(mapped = true, nonDuplicate = true, passedVendorQualityChecks = true)
    (loadReads(sc, tumorFile, filters = filters).mappedReads.collect(), loadReads(sc, normalFile, filters = filters).mappedReads.collect())
  }

  def loadReads(sc: SparkContext,
                filename: String,
                filters: Read.InputFilters = Read.InputFilters.empty): ReadSet = {
    /* grab the path to the SAM file we've stashed in the resources subdirectory */
    val path = testDataPath(filename)
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    ReadSet(sc, path, filters = filters)
  }

  def loadTumorNormalPileup(tumorReads: Seq[MappedRead],
                            normalReads: Seq[MappedRead],
                            locus: Long): (Pileup, Pileup) = {
    (Pileup(tumorReads, locus), Pileup(normalReads, locus))
  }

  def assertAlmostEqual(a: Double, b: Double, epsilon: Double = 1e-6) {
    assert(abs(a - b) < epsilon)
  }

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

  /**
   * Delete a file or directory (recursively) if it exists.
   */
  def deleteIfExists(filename: String) = {
    val file = new File(filename)
    try {
      FileUtils.forceDelete(file)
    } catch {
      case e: FileNotFoundException => {}
    }
  }

  trait SparkFunSuite extends FunSuite with BeforeAndAfter {

    val sparkPortProperty = "spark.driver.port"

    var sc: SparkContext = _
    var maybeLevels: Option[Map[String, Level]] = None

    def createSpark(sparkName: String, silenceSpark: Boolean = true) = {
      // Silence the Spark logs if requested
      maybeLevels = if (silenceSpark) Some(SparkLogUtil.silenceSpark()) else None
      synchronized {
        // Find two unused ports
        val driverSocket = new ServerSocket(0)
        val uiSocket = new ServerSocket(0)
        val driverPort = driverSocket.getLocalPort
        val uiPort = uiSocket.getLocalPort
        driverSocket.close()
        uiSocket.close()
        val conf = new SparkConf(false)
          .setAppName("guacamole: " + sparkName)
          .setMaster("local[4]")
          .set(sparkPortProperty, driverPort.toString)
          .set("spark.ui.port", uiPort.toString)
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "org.bdgenomics.guacamole.GuacamoleKryoRegistrator")
          .set("spark.kryoserializer.buffer.mb", "4")
          .set("spark.kryo.referenceTracking", "true")
        sc = new SparkContext(conf)
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

    def sparkTest(name: String, silenceSpark: Boolean = true)(body: => Unit) {
      if (runOnly.isEmpty || runOnly == name) {
        test(name, SparkTest) {
          createSpark(name, silenceSpark)
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
}
