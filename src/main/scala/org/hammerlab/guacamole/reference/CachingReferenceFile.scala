//package org.hammerlab.guacamole.reference
//
//import org.hammerlab.genomics.bases.Bases
//import org.hammerlab.genomics.reference.{ ContigName, ContigSequence, Locus, NumLoci }
//import org.hammerlab.paths.Path
//
//case class CachingReferenceFile(blockSize: Int,
//                                wrapped: ReferenceGenome)
//  extends ReferenceGenome {
//  override def source: Option[Path] = wrapped.source
//
//  private val contigs = collection.concurrent.TrieMap[ContigName, ContigSequence]()
//
//  override def apply(contigName: ContigName): ContigSequence =
//    contigs.getOrElseUpdate(
//      contigName,
//      CachingContig(blockSize, wrapped(contigName))
//    )
//}
//
//case class CachingContig(blockSize: Int,
//                         wrapped: ContigSequence)
//  extends ContigSequence {
//
//  private val cache = collection.concurrent.TrieMap[Int, Bases]()
//
//  override def slice(start: Locus, length: Int): Bases = ???
//  override def length: NumLoci = wrapped.length
//}
