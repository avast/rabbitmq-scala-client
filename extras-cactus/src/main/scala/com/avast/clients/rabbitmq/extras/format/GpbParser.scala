package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.google.protobuf.{MessageLite, Parser}

import scala.reflect.ClassTag
import scala.util.Try

trait GpbParser[A <: MessageLite] {
  def parseFrom(bytes: Bytes): Try[A]
}

object GpbParser {
  implicit def parserForGpb[Gpb <: MessageLite: ClassTag]: GpbParser[Gpb] = {
    val gpbClass = implicitly[ClassTag[Gpb]].runtimeClass
    val parser = gpbClass.getMethod("getDefaultInstance").invoke(gpbClass).asInstanceOf[Gpb].getParserForType.asInstanceOf[Parser[Gpb]]

    (bytes: Bytes) =>
      Try(parser.parseFrom(bytes.newInputStream()))
  }
}
