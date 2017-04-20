package org.hammerlab.guacamole.reference

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.Files

import org.hammerlab.paths.Path

trait SeekableReadable {
  def seek(pos: Long): this.type
  def read(buf: ByteBuffer): Int
  def size: Long
}

trait IsSeekableReadable[T]
  extends Function1[T, SeekableReadable]

object SeekableReadable {

  implicit class SeekableReadableByteChannel(channel: SeekableByteChannel)
    extends SeekableReadable {
    override def seek(pos: Long): this.type = { channel.position(pos); this }
    override def read(buf: ByteBuffer): Int = channel.read(buf)
    override def size: Long = channel.size
  }

  implicit object SeekableReadableByteChannel
    extends IsSeekableReadable[Path] {
    override def apply(path: Path): SeekableReadable = SeekableReadableByteChannel(Files.newByteChannel(path))
  }
}
