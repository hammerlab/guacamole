/**
 * Copyright 2014. Regents of the University of California.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.guacamole

import org.bdgenomics.adam.avro.{ADAMRecord, ADAMVariant}
import org.scalatest._
import org.scalatest.matchers.{ ShouldMatchers }
import org.bdgenomics.adam.rich.DecadentRead
import org.bdgenomics.adam.rdd.ADAMContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class PileupSuite extends TestUtil.SparkFunSuite with ShouldMatchers {

  def loadPileup(filename : String, locus : Long = 0) : Pileup = {
    val sc = new SparkContext("local", "localPileupTest")
    val adamContext = ADAMContext.sparkContextToADAMContext(sc)

    /* grab the path to the SAM file we've stashed in the resources subdirectory */
    val path = ClassLoader.getSystemClassLoader.getResource(filename).getFile

    val records : RDD[ADAMRecord] = adamContext.adamLoad(path)

    /* convert the jack-of-all-trades ADAMRecord to DecadentRead,
       which seems to have more read-specific non-optional fields
     */
    val distributedDecadentReads = records.map(DecadentRead.apply)

    /* convert from a distributed RDD to a local array */
    val localDecadentReads = distributedDecadentReads.collect()
    sc.stop()
    Pileup(localDecadentReads, locus)
   }


  val pileup = loadPileup("same_start_reads.sam", 0)

  test("Load pileup from SAM file") {
    pileup.elements.length should be(10)
  }

  val emptyReadSeq = Seq()

  test("First 60 loci should have all 10 reads") {
    /* First ten loci involve all reads */
    for (i <- 1 to 59) {
      val nextPileup = pileup.atGreaterLocus(i, emptyReadSeq.iterator)
      nextPileup.elements.length should be(10)
    }
  }

  test("Loci 10-19 deleted from half of the reads") {
    /* First ten loci involve all reads */
    for (i <- 10 to 19) {
      val nextPileup = pileup.atGreaterLocus(i, emptyReadSeq.iterator)
      nextPileup.elements.filter(_.isDeletion).length should be(5)
    }
  }

  test("Loci 60-69 have 5 reads") {
    /* First ten loci involve all reads */
    for (i <- 60 to 69) {
      val nextPileup = pileup.atGreaterLocus(i, emptyReadSeq.iterator)

      nextPileup.elements.length should be(5)
    }
  }
}
