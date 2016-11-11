package org.hammerlab.guacamole.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord, VariantContext}
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator
import org.hammerlab.guacamole.jointcaller.kryo.{Registrar => JointCallerRegistrar}
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.map.{LociMap, Contig => LociMapContig, ContigSerializer => LociMapContigSerializer, Serializer => LociMapSerializer}
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.PartitionIndex
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.loci.partitioning.MicroRegionPartitioner.MicroPartitionIndex
import org.hammerlab.guacamole.loci.set.{LociSet, Contig => LociSetContig, ContigSerializer => LociSetContigSerializer, Serializer => LociSetSerializer}
import org.hammerlab.guacamole.reads.{MappedRead, MappedReadSerializer, MateAlignmentProperties, PairedRead, Read, UnmappedRead, UnmappedReadSerializer}
import org.hammerlab.guacamole.readsets.ContigLengths
import org.hammerlab.guacamole.reference.Position
import org.hammerlab.guacamole.variants.{Allele, AlleleEvidence, AlleleSerializer, CalledAllele, CalledSomaticAllele, Genotype}
import org.hammerlab.magic.accumulables.{HashMap => MagicHashMap}
import org.hammerlab.magic.kryo.{Registrar => MagicRDDRegistrar}

class Registrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {

    // Register ADAM serializers.
    new ADAMKryoRegistrator().registerClasses(kryo)

    new MagicRDDRegistrar().registerClasses(kryo)

    // Register Joint-Caller serializers.
    new JointCallerRegistrar().registerClasses(kryo)

    // SequenceDictionary (and its records) are serialized when loading Reads from ADAM, in
    // Read.ADAMSequenceDictionaryRDDAggregator. ADAM should register these itself.
    kryo.register(classOf[SequenceDictionary])
    kryo.register(classOf[SequenceRecord])

    kryo.register(classOf[ContigLengths])

    // Reads
    kryo.register(classOf[MappedRead], new MappedReadSerializer)
    kryo.register(classOf[Array[MappedRead]])
    kryo.register(classOf[MateAlignmentProperties])
    kryo.register(classOf[Array[Read]])
    kryo.register(classOf[UnmappedRead], new UnmappedReadSerializer)

    kryo.register(classOf[PairedRead[_]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofByte])  // PairedRead

    // LociSet is serialized when broadcast in InputConfig.filterRDD. Serde'ing a LociSet delegates to an Array of
    // Contigs.
    kryo.register(classOf[LociSet], new LociSetSerializer)
    kryo.register(classOf[Array[LociSet]])
    kryo.register(classOf[LociSetContig], new LociSetContigSerializer)
    kryo.register(classOf[Array[LociSetContig]])

    kryo.register(classOf[Coverage])

    Position.registerKryo(kryo)

    // LociMap is serialized when broadcast in LociPartitionUtils.partitionLociByApproximateDepth.
    kryo.register(classOf[LociPartitioning])
    kryo.register(classOf[LociMap[PartitionIndex]], new LociMapSerializer[PartitionIndex])
    kryo.register(classOf[LociMapContig[PartitionIndex]], new LociMapContigSerializer[PartitionIndex])
    kryo.register(classOf[LociMapContig[MicroPartitionIndex]], new LociMapContigSerializer[MicroPartitionIndex])

    kryo.register(classOf[MagicHashMap[_, _]])

    // Germline-assembly caller flatmaps some CalledAlleles.
    kryo.register(classOf[Array[CalledAllele]])
    kryo.register(classOf[CalledAllele])
    kryo.register(classOf[Genotype])
    kryo.register(classOf[Allele], new AlleleSerializer)
    kryo.register(classOf[AlleleEvidence])

    // Seems to be used when collecting RDDs of Objects of various kinds.
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])

    // https://mail-archives.apache.org/mod_mbox/spark-user/201504.mbox/%3CCAC95X6JgXQ3neXF6otj6a+F_MwJ9jbj9P-Ssw3Oqkf518_eT1w@mail.gmail.com%3E
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(classOf[java.lang.Class[_]])

    kryo.register(classOf[RichVariant])
    kryo.register(classOf[VariantContext])
    kryo.register(classOf[Array[String]])

    kryo.register(classOf[Array[CalledSomaticAllele]])
    kryo.register(classOf[CalledSomaticAllele])
  }
}
