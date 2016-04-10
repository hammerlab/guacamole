package org.hammerlab.guacamole.distributed

import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.{AccumulatorParam, SparkConf}

import scala.collection.mutable.{HashMap => MutableHashMap}

/**
  * Allows a mutable HashMap[String, Long] to be used as an accumulator in Spark.
  *
  * When we put (k, v2) into an accumulator that already contains (k, v1), the result will be a HashMap containing
  * (k, v1 + v2).
  *
  */
class HashMapAccumulatorParam extends AccumulatorParam[MutableHashMap[String, Long]] {
  /**
    * Combine two accumulators. Adds the values in each hash map.
    *
    * This method is allowed to modify and return the first value for efficiency.
    *
    * @see org.apache.spark.GrowableAccumulableParam.addInPlace(r1: R, r2: R): R
    *
    */
  def addInPlace(first: MutableHashMap[String, Long], second: MutableHashMap[String, Long]): MutableHashMap[String, Long] = {
    second.foreach(pair => {
      if (!first.contains(pair._1))
        first(pair._1) = pair._2
      else
        first(pair._1) += pair._2
    })
    first
  }

  /**
    * Zero value for the accumulator: the empty hash map.
    *
    * @see org.apache.spark.GrowableAccumulableParam.zero(initialValue: R): R
    *
    */
  def zero(initialValue: MutableHashMap[String, Long]): MutableHashMap[String, Long] = {
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[MutableHashMap[String, Long]](ser.serialize(initialValue))
    copy.clear()
    copy
  }
}
