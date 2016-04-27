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

package org.hammerlab.guacamole

import java.io.{File, InputStreamReader}

import htsjdk.variant.vcf.VCFFileReader
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Logging
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.set.{LociParser, LociSet}
import org.hammerlab.guacamole.readsets.ContigLengths

/**
 * Basic functions that most commands need, and specifications of command-line arguments that they use.
 *
 */
object Common extends Logging {

  /**
   * Return the loci specified by the user as a LociParser.
   *
   * @param args parsed arguments
   */
  def lociFromArguments(args: LociArgs, default: String = "all"): LociParser = {
    if (args.loci.nonEmpty && args.lociFromFile.nonEmpty) {
      throw new IllegalArgumentException("Specify at most one of the 'loci' and 'loci-from-file' arguments")
    }
    val lociToParse =
      if (args.loci.nonEmpty) {
        args.loci
      } else if (args.lociFromFile.nonEmpty) {
        // Load loci from file.
        val filesystem = FileSystem.get(new Configuration())
        val path = new Path(args.lociFromFile)
        IOUtils.toString(new InputStreamReader(filesystem.open(path)))
      } else {
        default
      }

    LociParser(lociToParse)
  }

  /**
   * Load a LociSet from the specified file, using the contig lengths from the given ReadSet.
   *
   * @param filePath path to file containing loci. If it ends in '.vcf' then it is read as a VCF and the variant sites
   *                 are the loci. If it ends in '.loci' or '.txt' then it should be a file containing loci as
   *                 "chrX:5-10,chr12-10-20", etc. Whitespace is ignored.
   * @param contigLengths contig lengths, by name
   * @return a LociSet
   */
  def lociFromFile(filePath: String, contigLengths: ContigLengths): LociSet = {
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
   * @param loci loci to load as a string
   * @param lociFromFilePath path to file containing loci to load
   * @param contigLengths contig lengths, by name
   * @return a LociSet
   */
  def loadLoci(loci: String, lociFromFilePath: String, contigLengths: ContigLengths): LociSet = {
    if (loci.nonEmpty && lociFromFilePath.nonEmpty) {
      throw new IllegalArgumentException("Specify at most one of the 'loci' and 'loci-from-file' arguments")
    }
    if (loci.nonEmpty) {
      LociParser(loci).result(contigLengths)
    } else if (lociFromFilePath.nonEmpty) {
      lociFromFile(lociFromFilePath, contigLengths)
    } else {
      // Default is "all"
      LociSet.all(contigLengths)
    }
  }
}

