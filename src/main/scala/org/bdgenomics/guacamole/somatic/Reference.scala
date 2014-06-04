package org.bdgenomics.guacamole.somatic

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.util.Utils
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.mapred.TextInputFormat
import scala.collection.mutable
import org.bdgenomics.guacamole.{ LociMapLongSingleContigSerializer, LociMap, Common }
import com.esotericsoftware.kryo.{ KryoSerializable, Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.apache.spark.broadcast.Broadcast

case class Reference(basesAtGlobalPositions: RDD[(Long, Byte)],
                     index: Reference.Index)

object Reference {

  type Locus = (String, Long)

  case class Index(contigSizes: Map[String, Long]) {
    val numLoci: Long = contigSizes.values.reduce(_ + _)
    val contigs = contigSizes.keys.toArray

    val contigIndices: Map[String, Int] = contigs.zipWithIndex.toMap
    val numContigs: Int = contigs.length
    val (_, contigStart) = contigs.foldLeft((0L, Map[String, Long]())) {
      case ((total, map), k) =>
        val n = contigSizes(k)
        val newTotal = total + n
        val newMap: Map[String, Long] = map + (k -> total)
        (newTotal, newMap)
    }

    val contigStartArray: Array[Long] = contigs.map {
      contig => contigStart(contig)
    }

    def globalPositionToLocus(globalPosition: Long): Reference.Locus = {
      val numContigs = contigStartArray.length
      val lastContigIndex = numContigs - 1

      var lower = 0
      var upper = lastContigIndex

      // binary search to find which locus the position belongs to
      while (lower != upper) {
        // midpoint between i & j in indices
        val middle = (upper + lower) / 2
        val currentPosition = contigStartArray(middle)
        if (currentPosition <= globalPosition) {
          // if the query position is between the current contig and the next one,
          // then just return the offset
          if ((middle < lastContigIndex) && (contigStartArray(middle + 1) > globalPosition)) {
            val currentContig = contigs(middle)
            return (currentContig, globalPosition - currentPosition)
          } else { lower = middle }
        } else { upper = middle }
      }
      assert(lower == upper)
      val startPosition = contigStartArray(lower)
      val contig = contigs(lower)
      (contig, globalPosition - startPosition)
    }

    def locusToGlobalPosition(locus: Reference.Locus): Long = {
      val (contig, offset) = locus
      val eltsBefore: Long = contigStart.getOrElse(contig, 0)
      eltsBefore + offset
    }

  }

  /**
   *
   * Since formats/sources differ on whether to call a chromosome "chr1" vs. "1"
   * normalize them all the drop the 'chr' prefix (and to use "M" instead of "MT").
   *
   * @param contigName
   * @return
   */
  def normalizeContigName(contigName: String): String = {
    contigName.replace("chr", "").replace("MT", "M")
  }

  /**
   * Loads a FASTA file into:
   * - RDD[(K,V)] where
   *   key K = (contig name : String, line in contig : Long)
   *   value V = string  of nucleotides
   * - Map[String,Long] of each contig name and its length
   *
   * @param path
   * @param sc
   * @return
   */
  def loadReferenceLines(path: String, sc: SparkContext, numSplits: Int): (RDD[(Locus, Array[Byte])], Map[String, Long]) = {

    // Hadoop loads a text file into an RDD of lines keyed by byte offsets
    val fastaByteOffsetsAndLines: RDD[(Long, Array[Byte])] =
      sc.hadoopFile[LongWritable, Text, TextInputFormat](path, numSplits).map({
        case (x, y) => (x.get(), y.getBytes)
      }).filter(_._2.length > 0)
    val sortedSequences: RDD[(Long, Array[Byte])] = fastaByteOffsetsAndLines.sortByKey(ascending = true).cache()

    val maxOffset = sortedSequences.map(pair => pair._1 + pair._2.length).reduce(Math.max)
    Common.progress("-- max offset in loaded reference lines = %d".format(maxOffset))
    //collect all the lines which start with '>'
    val referenceDescriptionLines: List[(Long, String)] =
      sortedSequences.filter({
        case (_, bytes) =>
          bytes.length > 0 && bytes(0).toChar == '>'
      }).collect.map({
        case (byteOffset, bytes) =>
          (byteOffset, bytes.map(_.toChar).mkString)
      }).toList

    val contigStarts = referenceDescriptionLines.map {
      case (byteOffset, description) =>
        //parse the contig description to just pull out the contig name
        val text = description.substring(1)
        val contigName = normalizeContigName(text.split(' ')(0))
        val seqStart = byteOffset + description.length + 1
        (contigName, seqStart)

    }
    Common.progress("-- collected contig headers")

    // start (inclusive) and stop (exclusive) bytes associated with each contig name
    val contigByteRanges = mutable.Map[String, (Long, Long)]()
    for ((contigName, seqStart) <- contigStarts) {
      val stopCandidates: List[Long] = contigStarts.filter(_._2 > seqStart).map(_._2)
      val seqStop: Long = if (stopCandidates.length > 0) stopCandidates.min else maxOffset
      contigByteRanges(contigName) = (seqStart, seqStop)
    }
    // hand-waiving around performance of closure objects by broadcasting
    // every collection that should be shared by multiple workers
    val byteRangesBroadcast = sc.broadcast(contigByteRanges)
    Common.progress("-- broadcast reference index")

    // associate each line with whichever contig contains it
    val locusLines = sortedSequences.flatMap({
      case (offset, seq) =>
        val maybeByteRange =
          byteRangesBroadcast.value.find({
            case (_, (start, stop)) =>
              (start <= offset && stop > offset)
          })
        maybeByteRange.map({
          case (contigName, (contigStart, _)) =>
            val startPosInContig = offset - contigStart
            ((contigName, startPosInContig), seq)
        })
    })
    val contigSizes: Map[String, Long] = contigByteRanges.mapValues(pair => pair._2 - pair._1).toMap
    (locusLines, contigSizes)
  }
  def makeBasesAtLoci(referenceLines: RDD[(Reference.Locus, Array[Byte])],
                      contigStart: Map[String, Long]) = {
    val broadcastContigStart = referenceLines.context.broadcast(contigStart)
    referenceLines.mapPartitions({
      iter =>
        val result = mutable.ArrayBuffer[(Long, Byte)]()
        while (iter.hasNext) {
          val ((contigName, readStart), bytes) = iter.next
          val contigStart: Long = broadcastContigStart.value(contigName)
          val start = contigStart + readStart
          var i = 0
          while (i < bytes.length) {
            result += ((start + i, bytes(i)))
          }
        }
        result.iterator
    }).cache()
  }

  /**
   * Load a FASTA reference file into a Reference object, which contans
   * an RDD of (Locus,Byte) for each nucleotide and a Map[Locus, (Long,Long)] of
   * start/stop
   * @param path
   * @param sc
   * @param numSplits
   * @return
   */
  def load(path: String, sc: SparkContext, numSplits: Option[Int] = None): Reference = {
    val n: Int = numSplits match {
      case None    => sc.defaultMinSplits
      case Some(n) => n
    }
    val (referenceLines, contigSizes) = loadReferenceLines(path, sc, n)
    val index = Index(contigSizes)
    val basesAtLoci = makeBasesAtLoci(referenceLines, index.contigStart)
    Reference(basesAtLoci, index)
  }
}