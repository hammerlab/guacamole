/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole.loci.set

import java.io.{File, InputStreamReader}
import java.lang.{Long => JLong}

import com.google.common.collect.{Range => JRange}
import htsjdk.variant.vcf.VCFFileReader
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.hammerlab.guacamole.readsets.ContigLengths
import org.hammerlab.guacamole.reference.{Locus, NumLoci}
import org.hammerlab.guacamole.reference.{ReferenceRegion, ContigName => ContigName}
import org.hammerlab.guacamole.strings.TruncatedToString

import scala.collection.JavaConversions._
import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

/**
 * An immutable collection of genomic regions on any number of contigs.
 *
 * Used, for example, to keep track of what loci to call variants at.
 *
 * Since contiguous genomic intervals are a common case, this is implemented with sets of (start, end) intervals.
 *
 * All intervals are half open: inclusive on start, exclusive on end.
 *
 * @param map A map from contig-name to Contig, which is a set or genomic intervals as described above.
 */
case class LociSet(private val map: SortedMap[ContigName, Contig]) extends TruncatedToString {

  /** The contigs included in this LociSet with a nonempty set of loci. */
  lazy val contigs = map.values.toArray

  /** The number of loci in this LociSet. */
  lazy val count: NumLoci = contigs.map(_.count).sum

  def isEmpty = map.isEmpty
  def nonEmpty = map.nonEmpty

  /** Given a contig name, returns a [[Contig]] giving the loci on that contig. */
  def onContig(name: ContigName): Contig = map.getOrElse(name, Contig(name))

  /** Build a truncate-able toString() out of underlying contig pieces. */
  def stringPieces: Iterator[String] = contigs.iterator.flatMap(_.stringPieces)

  def intersects(region: ReferenceRegion): Boolean =
    onContig(region.contigName).intersects(region.start, region.end)

  /**
   * Split the LociSet into two sets, where the first one has `numToTake` loci, and the second one has the
   * remaining loci.
   *
   * @param numToTake number of elements to take. Must be <= number of elements in the map.
   */
  def take(numToTake: NumLoci): (LociSet, LociSet) = {
    assume(numToTake <= count, s"Can't take $numToTake loci from a set of size $count.")

    // Optimize for taking none or all:
    if (numToTake == 0) {
      (LociSet(), this)
    } else if (numToTake == count) {
      (this, LociSet())
    } else {

      val first = new Builder
      val second = new Builder
      var remaining = numToTake
      var doneTaking = false

      for {
        contig <- contigs
      } {
        if (doneTaking) {
          second.add(contig)
        } else if (contig.count < remaining) {
          first.add(contig)
          remaining -= contig.count
        } else {
          val (takePartialContig, remainingPartialContig) = contig.take(remaining)
          first.add(takePartialContig)
          second.add(remainingPartialContig)
          doneTaking = true
        }
      }

      val (firstSet, secondSet) = (first.result, second.result)
      assert(firstSet.count == numToTake)
      assert(firstSet.count + secondSet.count == count)
      (firstSet, secondSet)
    }
  }
}

object LociSet {
  /** An empty LociSet. */
  def apply(): LociSet = LociSet(TreeMap.empty[ContigName, Contig])

  def all(contigLengths: ContigLengths) = LociParser.all.result(contigLengths)

  def apply(lociStr: String): LociSet = LociParser(lociStr).result

  /**
   * These constructors build a LociSet directly from Contigs.
   *
   * They operate on an Iterator so that transformations to the data happen in one pass.
   */
  private[set] def fromContigs(contigs: Iterable[Contig]): LociSet = fromContigs(contigs.iterator)
  private[set] def fromContigs(contigs: Iterator[Contig]): LociSet =
    LociSet(
      TreeMap(
        contigs
          .filterNot(_.isEmpty)
          .map(contig => contig.name -> contig)
          .toSeq: _*
      )
    )

  def apply(contigs: Iterable[(ContigName, Locus, Locus)]): LociSet =
    LociSet.fromContigs({
      (for {
        (name, start, end) <- contigs
        range = JRange.closedOpen[JLong](start, end)
        if !range.isEmpty
      } yield {
        name -> range
      })
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .map(Contig(_))
    })

  def apply(reader: VCFFileReader): LociSet =
    LociSet(
      reader
        .map(value =>
          (value.getContig, value.getStart - 1L, value.getEnd.toLong)
        )
    )

  /**
   * Load a LociSet from the specified file, using the contig lengths from the given ReadSet.
   *
   * @param filePath path to file containing loci. If it ends in '.vcf' then it is read as a VCF and the variant sites
   *                 are the loci. If it ends in '.loci' or '.txt' then it should be a file containing loci as
   *                 "chrX:5-10,chr12-10-20", etc. Whitespace is ignored.
   * @param contigLengths contig lengths, by name
   * @return a LociSet
   */
  private def loadFromFile(filePath: String, contigLengths: ContigLengths): LociSet = {
    if (filePath.endsWith(".vcf")) {
      LociSet(
        new VCFFileReader(new File(filePath), false)
      )
    } else if (filePath.endsWith(".loci") || filePath.endsWith(".txt")) {
      val filesystem = FileSystem.get(new Configuration())
      val path = new Path(filePath)
      LociParser(
        IOUtils.toString(new InputStreamReader(filesystem.open(path)))
      ).result(contigLengths)
    } else {
      throw new IllegalArgumentException(
        s"Couldn't guess format for file: $filePath. Expected file extensions: '.loci' or '.txt' for loci string format; '.vcf' for VCFs."
      )
    }
  }

  /**
   * Load loci from a string or a path to a file.
   *
   * Specify at most one of loci or lociFromFilePath.
   *
   * @param lociStr loci to load as a string
   * @param lociFromFilePath path to file containing loci to load
   * @param contigLengths contig lengths, by name
   * @return a LociSet
   */
  def load(lociStr: String, lociFromFilePath: String, contigLengths: ContigLengths): LociSet = {
    if (lociStr.nonEmpty && lociFromFilePath.nonEmpty) {
      throw new IllegalArgumentException("Specify at most one of the 'loci' and 'loci-from-file' arguments")
    }
    if (lociStr.nonEmpty) {
      LociParser(lociStr).result(contigLengths)
    } else if (lociFromFilePath.nonEmpty) {
      loadFromFile(lociFromFilePath, contigLengths)
    } else {
      // Default is "all"
      LociSet.all(contigLengths)
    }
  }
}
