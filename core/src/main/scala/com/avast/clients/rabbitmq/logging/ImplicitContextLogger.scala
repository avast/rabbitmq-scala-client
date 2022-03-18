package com.avast.clients.rabbitmq.logging

import cats.effect.Sync

import scala.reflect.ClassTag

private[rabbitmq] class ImplicitContextLogger[F[_]](private val contextLessLogger: GenericPlainLogger[F]) {

  // contextLessLogger.info()
  def info[Ctx <: LoggingContext](msg: => String)(implicit ctx: Ctx): F[Unit] = contextLessLogger.info(ctx.asContextMap)(msg)
  def info[Ctx <: LoggingContext](t: Throwable)(msg: => String)(implicit ctx: Ctx): F[Unit] =
    contextLessLogger.info(ctx.asContextMap, t)(msg)
  def plainInfo(msg: => String): F[Unit] = contextLessLogger.info(msg)
  def plainInfo(t: Throwable)(msg: => String): F[Unit] = contextLessLogger.info(t)(msg)

  // contextLessLogger.warn()
  def warn[Ctx <: LoggingContext](msg: => String)(implicit ctx: Ctx): F[Unit] = contextLessLogger.warn(ctx.asContextMap)(msg)
  def warn[Ctx <: LoggingContext](t: Throwable)(msg: => String)(implicit ctx: Ctx): F[Unit] =
    contextLessLogger.warn(ctx.asContextMap, t)(msg)
  def plainWarn(msg: => String): F[Unit] = contextLessLogger.warn(msg)
  def plainWarn(t: Throwable)(msg: => String): F[Unit] = contextLessLogger.warn(t)(msg)

  // contextLessLogger.error()
  def error[Ctx <: LoggingContext](msg: => String)(implicit ctx: Ctx): F[Unit] = contextLessLogger.error(ctx.asContextMap)(msg)
  def error[Ctx <: LoggingContext](t: Throwable)(msg: => String)(implicit ctx: Ctx): F[Unit] =
    contextLessLogger.error(ctx.asContextMap, t)(msg)
  def plainError(msg: => String): F[Unit] = contextLessLogger.error(msg)
  def plainError(t: Throwable)(msg: => String): F[Unit] = contextLessLogger.error(t)(msg)

  // contextLessLogger.debug()
  def debug[Ctx <: LoggingContext](msg: => String)(implicit ctx: Ctx): F[Unit] = contextLessLogger.debug(ctx.asContextMap)(msg)
  def debug[Ctx <: LoggingContext](t: Throwable)(msg: => String)(implicit ctx: Ctx): F[Unit] =
    contextLessLogger.debug(ctx.asContextMap, t)(msg)
  def plainDebug(msg: => String): F[Unit] = contextLessLogger.debug(msg)
  def plainDebug(t: Throwable)(msg: => String): F[Unit] = contextLessLogger.debug(t)(msg)

  // contextLessLogger.trace()
  def trace[Ctx <: LoggingContext](msg: => String)(implicit ctx: Ctx): F[Unit] = contextLessLogger.trace(ctx.asContextMap)(msg)
  def trace[Ctx <: LoggingContext](t: Throwable)(msg: => String)(implicit ctx: Ctx): F[Unit] =
    contextLessLogger.trace(ctx.asContextMap, t)(msg)
  def plainTrace(msg: => String): F[Unit] = contextLessLogger.trace(msg)
  def plainTrace(t: Throwable)(msg: => String): F[Unit] = contextLessLogger.trace(t)(msg)
}

private[rabbitmq] object ImplicitContextLogger {
  def createLogger[F[_]: Sync, A](implicit ct: ClassTag[A]): ImplicitContextLogger[F] =
    new ImplicitContextLogger(createPlainLogger[F, A])
}
