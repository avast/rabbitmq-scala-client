package com.avast.clients.rabbitmq.logging

import cats.effect.Sync
import com.avast.clients.rabbitmq.api.CorrelationId

import scala.reflect.ClassTag

class ImplicitContextLogger[F[_]](private val contextLessLogger: GenericPlainLogger[F]) {
  // contextLessLogger.info()
  def info[A](msg: => String)(implicit cid: CorrelationId): F[Unit] = contextLessLogger.info(cid.asContextMap)(msg)
  def info[A](t: Throwable)(msg: => String)(implicit cid: CorrelationId): F[Unit] = contextLessLogger.info(cid.asContextMap, t)(msg)
  def plainInfo(msg: => String): F[Unit] = contextLessLogger.info(msg)
  def plainInfo(t: Throwable)(msg: => String): F[Unit] = contextLessLogger.info(t)(msg)

  // contextLessLogger.warn()
  def warn[A](msg: => String)(implicit cid: CorrelationId): F[Unit] = contextLessLogger.warn(cid.asContextMap)(msg)
  def warn[A](t: Throwable)(msg: => String)(implicit cid: CorrelationId): F[Unit] = contextLessLogger.warn(cid.asContextMap, t)(msg)
  def plainWarn(msg: => String): F[Unit] = contextLessLogger.warn(msg)
  def plainWarn(t: Throwable)(msg: => String): F[Unit] = contextLessLogger.warn(t)(msg)

  // contextLessLogger.error()
  def error[A](msg: => String)(implicit cid: CorrelationId): F[Unit] = contextLessLogger.error(cid.asContextMap)(msg)
  def error[A](t: Throwable)(msg: => String)(implicit cid: CorrelationId): F[Unit] = contextLessLogger.error(cid.asContextMap, t)(msg)
  def plainError(msg: => String): F[Unit] = contextLessLogger.error(msg)
  def plainError(t: Throwable)(msg: => String): F[Unit] = contextLessLogger.error(t)(msg)

  // contextLessLogger.debug()
  def debug[A](msg: => String)(implicit cid: CorrelationId): F[Unit] = contextLessLogger.debug(cid.asContextMap)(msg)
  def debug[A](t: Throwable)(msg: => String)(implicit cid: CorrelationId): F[Unit] = contextLessLogger.debug(cid.asContextMap, t)(msg)
  def plainDebug(msg: => String): F[Unit] = contextLessLogger.debug(msg)
  def plainDebug(t: Throwable)(msg: => String): F[Unit] = contextLessLogger.debug(t)(msg)

  // contextLessLogger.trace()
  def trace[A](msg: => String)(implicit cid: CorrelationId): F[Unit] = contextLessLogger.trace(cid.asContextMap)(msg)
  def trace[A](t: Throwable)(msg: => String)(implicit cid: CorrelationId): F[Unit] = contextLessLogger.trace(cid.asContextMap, t)(msg)
  def plainTrace(msg: => String): F[Unit] = contextLessLogger.trace(msg)
  def plainTrace(t: Throwable)(msg: => String): F[Unit] = contextLessLogger.trace(t)(msg)
}

object ImplicitContextLogger {
  def createLogger[F[_]: Sync, A](implicit ct: ClassTag[A]): ImplicitContextLogger[F] =
    new ImplicitContextLogger(createPlainLogger[F, A])
}
