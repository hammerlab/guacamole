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

package org.hammerlab.guacamole.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.storage.BroadcastBlockId
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator
import org.bdgenomics.formats.avro.{AlignmentRecord, Genotype => BDGGenotype}
import org.hammerlab.guacamole.commands.VariantLocus
import org.hammerlab.guacamole.commands.jointcaller.Parameters.SomaticGenotypePolicy
import org.hammerlab.guacamole.commands.jointcaller.annotation.{InsufficientNormal, MultiSampleAnnotations, SingleSampleAnnotations, StrandBias}
import org.hammerlab.guacamole.commands.jointcaller.evidence.{MultiSampleMultiAlleleEvidence, MultiSampleSingleAlleleEvidence, NormalDNASingleSampleSingleAlleleEvidence, TumorDNASingleSampleSingleAlleleEvidence, TumorRNASingleSampleSingleAlleleEvidence}
import org.hammerlab.guacamole.commands.jointcaller.{AlleleAtLocus, Input, InputCollection, Parameters}
import org.hammerlab.guacamole.distributed.LociPartitionUtils.{LociPartitioning, PartitionIdx}
import org.hammerlab.guacamole.distributed.TaskPosition
import org.hammerlab.guacamole.loci.SimpleRange
import org.hammerlab.guacamole.loci.map.{Contig => LociMapContig, ContigSerializer => LociMapContigSerializer, Serializer => LociMapSerializer}
import org.hammerlab.guacamole.loci.set.{LociSet, Contig => LociSetContig, ContigSerializer => LociSetContigSerializer, Serializer => LociSetSerializer}
import org.hammerlab.guacamole.pileup.{Pileup, PileupElement}
import org.hammerlab.guacamole.reads.{MappedRead, MappedReadSerializer, MateAlignmentProperties, PairedRead, Read, UnmappedRead, UnmappedReadSerializer}
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.variants._

class GuacamoleKryoRegistrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {

    // Register ADAM serializers.
    new ADAMKryoRegistrator().registerClasses(kryo)

    kryo.register(classOf[Array[BDGGenotype]])
    kryo.register(classOf[SequenceDictionary])
    kryo.register(classOf[SequenceRecord])

    kryo.register(classOf[PileupElement])
    kryo.register(classOf[Array[PileupElement]])

    kryo.register(classOf[NormalDNASingleSampleSingleAlleleEvidence])
    kryo.register(classOf[TumorDNASingleSampleSingleAlleleEvidence])

    // These are needed for MultiSampleSingleAlleleEvidence; perTumorDnaSampleTopMixtures and probably other fields.
    kryo.register(Class.forName("scala.collection.immutable.MapLike$$anon$2"))  // MapLike.MappedValues?
    kryo.register(Class.forName("org.hammerlab.guacamole.commands.jointcaller.evidence.MultiSampleSingleAlleleEvidence$$anonfun$16"))
    kryo.register(Class.forName("org.hammerlab.guacamole.commands.jointcaller.evidence.MultiSampleSingleAlleleEvidence$$anonfun$10"))

    kryo.register(classOf[Parameters])
    kryo.register(Class.forName("scala.Enumeration$Val"))
    kryo.register(SomaticGenotypePolicy.getClass)
    kryo.register(classOf[MultiSampleAnnotations])
    kryo.register(classOf[InputCollection])
    kryo.register(classOf[Input])
    kryo.register(Input.Analyte.getClass)
    kryo.register(Input.TissueType.getClass)
    kryo.register(classOf[InsufficientNormal])
    kryo.register(classOf[TumorRNASingleSampleSingleAlleleEvidence])
    kryo.register(Class.forName("org.apache.spark.broadcast.TorrentBroadcast"))
    kryo.register(classOf[BroadcastBlockId])
    kryo.register(classOf[Parameters.SomaticGenotypePolicy.Value])
    kryo.register(classOf[StrandBias])
    kryo.register(classOf[SingleSampleAnnotations])
    kryo.register(classOf[AlleleAtLocus])
    kryo.register(classOf[MultiSampleSingleAlleleEvidence])
    kryo.register(classOf[MultiSampleMultiAlleleEvidence])
    kryo.register(classOf[Array[MultiSampleMultiAlleleEvidence]])

    kryo.register(classOf[Pileup])
    kryo.register(classOf[Array[Pileup]])

    kryo.register(classOf[Array[Seq[_]]])

    kryo.register(classOf[MapBackedReferenceSequence])

    // Register Guacamole serializers.
    kryo.register(classOf[MappedRead], new MappedReadSerializer)
    kryo.register(classOf[Array[MappedRead]])
    kryo.register(classOf[PairedRead[_]])
    kryo.register(classOf[MateAlignmentProperties])
    kryo.register(classOf[Array[Read]])
    kryo.register(classOf[Array[VariantLocus]])
    kryo.register(classOf[VariantLocus])
    kryo.register(classOf[UnmappedRead], new UnmappedReadSerializer)

    kryo.register(classOf[LociSet], new LociSetSerializer)
    kryo.register(classOf[LociSetContig], new LociSetContigSerializer)
    kryo.register(classOf[Array[LociSetContig]])

    kryo.register(classOf[LociPartitioning], new LociMapSerializer[PartitionIdx])
    kryo.register(classOf[LociMapContig[PartitionIdx]], new LociMapContigSerializer[PartitionIdx])

    kryo.register(classOf[Allele], new AlleleSerializer)
    kryo.register(classOf[Genotype], new GenotypeSerializer)
    kryo.register(classOf[TaskPosition])

    // Germline-assembly caller flatmaps some CalledAlleles.
    kryo.register(classOf[Array[CalledAllele]])
    kryo.register(classOf[CalledAllele])
    kryo.register(classOf[AlleleEvidence])

    kryo.register(classOf[Array[LociSet]])
    kryo.register(classOf[Map[_, _]])
    kryo.register(Map.empty.getClass)
    kryo.register(scala.math.Numeric.LongIsIntegral.getClass)
    kryo.register(classOf[SimpleRange])
    kryo.register(classOf[Array[SimpleRange]])

    kryo.register(classOf[Vector[_]])
    kryo.register(classOf[Array[Vector[_]]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofLong])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofByte])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofChar])
    kryo.register(classOf[Array[Char]])

    // Tuple2[Long, Any], afaict?
    // "J" == Long (obviously). https://github.com/twitter/chill/blob/6d03f6976f33f6e2e16b8e254fead1625720c281/chill-scala/src/main/scala/com/twitter/chill/TupleSerializers.scala#L861
    kryo.register(Class.forName("scala.Tuple2$mcJZ$sp"))
    kryo.register(Class.forName("scala.Tuple2$mcIZ$sp"))

    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[Int]])

    // https://mail-archives.apache.org/mod_mbox/spark-user/201504.mbox/%3CCAC95X6JgXQ3neXF6otj6a+F_MwJ9jbj9P-Ssw3Oqkf518_eT1w@mail.gmail.com%3E
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(classOf[java.lang.Class[_]])

    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[Array[Array[Byte]]])

    kryo.register(classOf[Array[AlignmentRecord]])
    kryo.register(classOf[ReferenceRegion])
    kryo.register(classOf[Array[ReferenceRegion]])

    kryo.register(classOf[org.bdgenomics.formats.avro.Strand])
    kryo.register(classOf[org.bdgenomics.adam.models.MultiContigNonoverlappingRegions])
    kryo.register(classOf[org.bdgenomics.adam.models.NonoverlappingRegions])

    kryo.register(classOf[Array[Object]])
  }
}
