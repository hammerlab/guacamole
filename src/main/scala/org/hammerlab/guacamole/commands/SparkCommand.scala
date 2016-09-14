package org.hammerlab.guacamole.commands

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

abstract class SparkCommand[T <: Args: Manifest] extends Command[T] {
  override def run(args: T): Unit = {
    val sc = createSparkContext()
    try {
      args.validate(sc)
      run(args, sc)
    } finally {
      sc.stop()
    }
  }

  def run(args: T, sc: SparkContext): Unit

  private val defaultConfs = mutable.HashMap[String, String]()
  def setDefaultConf(key: String, value: String): Unit = {
    defaultConfs.update(key, value)
  }

  /**
   * Return a spark context.
   *
   * Typically, most properties are set through config file / cmd-line.
   * @return
   */
  def createSparkContext(): SparkContext = {
    val config: SparkConf = new SparkConf()

    config.getOption("spark.app.name") match {
      case Some(cmdLineName) => config.setAppName(s"$cmdLineName: $name")
      case _                 => config.setAppName(s"guacamole: $name")
    }

    if (config.getOption("spark.master").isEmpty) {
      val numProcessors = Runtime.getRuntime.availableProcessors()
      config.setMaster(s"local[$numProcessors]")
      info(s"Running in local mode with $numProcessors 'executors'")
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
