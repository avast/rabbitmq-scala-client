package com.avast.clients.rabbitmq

import cats.effect.Sync
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.reflect.ClassTag

package object logging {
  type GenericPlainLogger[F[_]] = SelfAwareStructuredLogger[F]

  def createPlainLogger[F[_]: Sync, A](implicit ct: ClassTag[A]): GenericPlainLogger[F] =
    Slf4jLogger.getLoggerFromClass(ct.runtimeClass)
}
