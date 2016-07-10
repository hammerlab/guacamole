package org.hammerlab.guacamole.commands

import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.utils.cli.Args4jBase

import scala.collection.mutable

abstract class SparkCommand[T <: Args4jBase: Manifest] extends Command[T] {
  override def run(args: T): Unit = {
    val sc = createSparkContext(appName = name)
    try {
      run(args, sc)
    } finally {
      sc.stop()
    }
  }

  def run(args: T, sc: SparkContext): Unit

  private val defaultConfs = mutable.HashMap[String, String]()
  protected def setDefaultConf(key: String, value: String): Unit = {
    defaultConfs.update(key, value)
  }

  /**
   *
   * Return a spark context.
   *
   * NOTE: Most properties are set through config file
   *
   * @param appName
   * @return
   */
  def createSparkContext(appName: String): SparkContext = createSparkContext(Some(appName))
  def createSparkContext(appName: Option[String] = None): SparkContext = {
    val config: SparkConf = new SparkConf()

    appName match {
      case Some(name) => config.setAppName("guacamole: %s".format(name))
      case _          => config.setAppName("guacamole")
    }

    if (config.getOption("spark.master").isEmpty) {
      config.setMaster("local[%d]".format(Runtime.getRuntime.availableProcessors()))
    }

    if (config.getOption("spark.serializer").isEmpty) {
      config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    }

    if (config.getOption("spark.kryo.registrator").isEmpty) {
      config.set("spark.kryo.registrator", "org.hammerlab.guacamole.kryo.Registrar")
    }

    if (config.getOption("spark.kryoserializer.buffer").isEmpty) {
      config.set("spark.kryoserializer.buffer", "4mb")
    }

    if (config.getOption("spark.kryo.referenceTracking").isEmpty) {
      config.set("spark.kryo.referenceTracking", "true")
    }

    for {
      (k, v) <- defaultConfs
    } {
      config.set(k, v)
    }

    new SparkContext(config)
  }
}
