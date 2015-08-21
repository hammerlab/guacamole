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

package org.hammerlab.guacamole.variants

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.Bases.BasesOrdering

case class Allele(refBases: Seq[Byte], altBases: Seq[Byte]) extends Ordered[Allele] {
  lazy val isVariant = refBases != altBases

  override def toString: String = "Allele(%s,%s)".format(Bases.basesToString(refBases), Bases.basesToString(altBases))

  override def compare(that: Allele): Int = {
    BasesOrdering.compare(refBases, that.refBases) match {
      case 0 => BasesOrdering.compare(altBases, that.altBases)
      case x => x
    }
  }
}

object Allele {
  def apply(refBases: String, altBases: String): Allele = {
    Allele(Bases.stringToBases(refBases), Bases.stringToBases(altBases))
  }
}

class AlleleSerializer extends Serializer[Allele] {
  def write(kryo: Kryo, output: Output, obj: Allele) = {
    output.writeInt(obj.refBases.length, true)
    output.writeBytes(obj.refBases.toArray)
    output.writeInt(obj.altBases.length, true)
    output.writeBytes(obj.altBases.toArray)
  }

  def read(kryo: Kryo, input: Input, klass: Class[Allele]): Allele = {
    val referenceBasesLength = input.readInt(true)
    val referenceBases: Seq[Byte] = input.readBytes(referenceBasesLength)
    val alternateLength = input.readInt(true)
    val alternateBases: Seq[Byte] = input.readBytes(alternateLength)
    Allele(referenceBases, alternateBases)
  }
}

trait HasAlleleSerializer {
  lazy val alleleSerializer: AlleleSerializer = new AlleleSerializer
}
