package org.hammerlab.guacamole.jointcaller

//import org.apache.hadoop.fs.{ Path ⇒ HPath }
import org.apache.hadoop.fs.Path
import org.hammerlab.genomics.readsets
import org.hammerlab.genomics.readsets.io.Sample.{ Id, Name }
import org.hammerlab.guacamole.jointcaller.Sample.{ Analyte, TissueType }

// Need a serializable Path
//case class Path(path: String)
//  extends HPath(path)

case class Input(override val id: Id,
                 override val name: Name,
                 path: Path,
                 override val tissueType: TissueType.Value,
                 override val analyte: Analyte.Value)
  extends Sample(id, name, tissueType, analyte)
    with readsets.io.Input

