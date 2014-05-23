package org.bdgenomics.guacamole.somatic

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.mapred.TextInputFormat
import scala.collection.mutable

object Reference {

  type Locus = (String, Long)

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
   * Loads a FASTA file into an RDD[(K,V)] where
   * key K = (contig name : String, line in contig : Long)
   * value V = string  of nucleotides
   *
   * @param path
   * @param sc
   * @return
   */
  def loadReferenceLines(path: String, sc: SparkContext): RDD[(Locus, Array[Byte])] = {

    // Hadoop loads a text file into an RDD of lines keyed by byte offsets
    val fastaByteOffsetsAndLines: RDD[(Long, Array[Byte])] =
      sc.hadoopFile[LongWritable, Text, TextInputFormat](path).map({
        case (x, y) => (x.get(), y.getBytes)
      })
    val sortedSequences: RDD[(Long, Array[Byte])] = fastaByteOffsetsAndLines.sortByKey(ascending = true)
    val numLines = sortedSequences.count
    val partitionSizes: Array[Long] = sortedSequences.mapPartitions({
      partition => Seq(partition.length.toLong).iterator
    }).collect()
    val partitionSizesBroadcast = sc.broadcast(partitionSizes)
    val numberedLines: RDD[(Long, Array[Byte])] = sortedSequences.mapPartitionsWithIndex {
      case (partitionIndex: Int, partition: Iterator[(Long, Array[Byte])]) =>
        val offset = if (partitionIndex > 0) partitionSizesBroadcast.value(partitionIndex - 1) else 0L
        partition.zipWithIndex.map({
          case ((_, bytes), i) => (i.toLong + offset, bytes)
        }).toIterator
    }
    assert(numberedLines.count == numLines)

    //collect all the lines which start with '>'
    val referenceDescriptionLines: List[(Long, String)] =
      numberedLines.filter({
        case (lineNumber, bytes) =>
          bytes.length > 0 && bytes(0).toChar == '>'
      }).collect.map({
        case (lineNumber, bytes) =>
          (lineNumber, bytes.map(_.toChar).mkString)
      }).toList

    //parse the contig description to just pull out the contig name
    val referenceContigNames: List[(Long, String)] =
      referenceDescriptionLines.map({
        case (lineNumber, description) =>
          val text = description.substring(1)
          val contigName = normalizeContigName(text.split(' ')(0))
          (lineNumber, contigName)
      })
    val referenceContigBytes = referenceContigNames.map(_._1)
    val referenceIndex = mutable.Map[String, (Long, Long)]()
    for ((start, contigName) <- referenceContigNames) {
      // stop of this contig is the start line of the next one
      val stopCandidates = referenceContigBytes.filter(_ > start)
      val stop = if (stopCandidates.length > 0) stopCandidates.min else numLines
      referenceIndex(contigName) = (start, stop)
    }
    // hand-waiving around performance of closure objects by broadcasting
    // every collection that should be shared by multiple workers
    val referenceIndexBroadcast = sc.broadcast(referenceIndex.toMap)

    // associate each line with whichever contig contains it
    numberedLines.flatMap({
      case (pos, seq) =>
        referenceIndexBroadcast.value.find({
          case (_, (start, stop)) =>
            (start < pos && stop > pos)
        }).map({
          case (contigName, (start, stop)) =>
            ((contigName, pos - start - 1), seq)
        })
    })
  }

  def loadReference(path: String, sc: SparkContext): RDD[(Locus, Byte)] = {
    val referenceLines: RDD[(Locus, Array[Byte])] = loadReferenceLines(path, sc)
    referenceLines.flatMap({ case (locus, bytes) => bytes.map((c: Byte) => (locus, c)) })
  }
}