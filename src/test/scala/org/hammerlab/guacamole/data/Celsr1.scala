package org.hammerlab.guacamole.data

import org.hammerlab.genomics.readsets.args.path.{ PathPrefix, UnprefixedPath }
import org.hammerlab.guacamole.jointcaller.Sample.Analyte.{ DNA, RNA }
import org.hammerlab.guacamole.jointcaller.Sample.TissueType.{ Normal, Tumor }
import org.hammerlab.guacamole.jointcaller.Sample.{ Analyte, TissueType }
import org.hammerlab.guacamole.jointcaller.{ Input, Inputs }
import org.hammerlab.test.resources.File

object Celsr1 {
  implicit val prefix = Some(PathPrefix("cancer-wes-and-rna-celsr1"))
  implicit def makeTissueType(str: String): TissueType.Value = TissueType.withName(str)
  implicit def makeAnalyte(str: String): Analyte.Value = Analyte.withName(str)

  def make(inputs: (String, TissueType.Value, Analyte.Value)*)(implicit prefixOpt: Option[PathPrefix]): Inputs =
    Inputs(
      for {
        ((path, tissueType, analyte), id) ← inputs.zipWithIndex.toVector
      } yield
        Input(
          id,
          path.toString,
          File(UnprefixedPath(path)),
          tissueType,
          analyte
        )
    )

  val inputs =
    make(
      (    "normal_0.bam", Normal, DNA),
      ( "tumor_wes_2.bam",  Tumor, DNA),
      ("tumor_rna_11.bam",  Tumor, RNA)
    )
}
