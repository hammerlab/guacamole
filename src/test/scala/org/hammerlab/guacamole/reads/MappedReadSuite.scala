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

package org.hammerlab.guacamole.reads

import htsjdk.samtools.TextCigarCodec
import org.hammerlab.guacamole.util.{ TestUtil, GuacFunSuite }
import TestUtil.Implicits._
import org.scalatest.Matchers

class MappedReadSuite extends GuacFunSuite with Matchers {

  test("mappedread is mapped") {
    val read = MappedRead(
      5, // token
      "read1",
      "TCGACCCTCGA",
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      "chr5",
      50,
      325352323,
      TextCigarCodec.decode(""),
      false,
      isPositiveStrand = true,
      isPaired = true
    )

    read.isMapped should be(true)
    read.asInstanceOf[Read].isMapped should be(true)

  }

  test("mixed collections mapped and unmapped reads") {
    val uread = UnmappedRead(
      5, // token
      "read1",
      "TCGACCCTCGA",
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      false,
      isPaired = true
    )

    val mread = MappedRead(
      5, // token
      "read1",
      "TCGACCCTCGA",
      Array[Byte]((10 to 20).map(_.toByte): _*),
      true,
      "some sample name",
      "chr5",
      50,
      325352323,
      TextCigarCodec.decode(""),
      false,
      isPositiveStrand = true,
      isPaired = true
    )
    //    test("Calculate mismatch qscore sum") {
    //      val originalReference = "AAATTGATACTCGAACGA"
    //      val read = TestUtil.makeRead(originalReference.substring(5, 15), "10M", "0C1C7", start = 5,
    //        qualityScores = Some(Seq(31, 10, 32, 10, 10, 10, 10, 10, 10, 10)))
    //      val mismatchQscoreSum = MDTagUtils.getMismatchingQscoreSum(read.mdTagOpt.get, read.baseQualities, read.cigar)
    //      mismatchQscoreSum should be(63)
    //    }
    val collectionMappedReads: Seq[Read] = Seq(uread, mread)
    collectionMappedReads(0).isMapped should be(false)
    collectionMappedReads(1).isMapped should be(true)
  }

}
