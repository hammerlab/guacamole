package org.hammerlab.guacamole.jointcaller

import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.{ Kryo, Serializer, io }
import org.apache.hadoop.fs.Path
import org.hammerlab.guacamole.jointcaller.Sample.{ Analyte, TissueType }

class InputSerializer
  extends Serializer[Input] {

  override def read(kryo: Kryo, input: io.Input, t: Class[Input]): Input = {
    Input(
      input.readInt(true),
      input.readString(),
      new Path(input.readString()),
      TissueType(input.readByte()),
      Analyte(input.readByte())
    )
  }

  override def write(kryo: Kryo, output: Output, input: Input): Unit = {
    output.writeInt(input.id, true)
    output.writeString(input.name)
    output.writeString(input.path.toString)
    output.writeByte(input.tissueType.id)
    output.writeByte(input.analyte.id)
  }
}
