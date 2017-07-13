package com.avast.clients.rabbitmq

import com.avast.clients.rabbitmq.HealthCheckStatus.{Failure, Ok}
import com.rabbitmq.client.{Channel, Connection, Consumer, ShutdownSignalException}
import com.typesafe.scalalogging.StrictLogging
import com.avast.clients.rabbitmq.api.{ChannelListener, ConnectionListener, ConsumerListener}

sealed trait HealthCheckStatus
object HealthCheckStatus {
  case object Ok extends HealthCheckStatus
  case class Failure(msg: String, f: Throwable) extends HealthCheckStatus
}

/*
 * The library is not able to recover from all failures so it provides this class that indicates if the application
 * is OK or not - then it should be restarted.
 * To use this class, simply pass the `rabbitExceptionHandler` as listener when constructing the RabbitMQ classes.
 * Then you can call `getStatus` method.
 */
class HealthCheck extends StrictLogging {

  // scalastyle:off
  private var status: HealthCheckStatus = Ok
  // scalastyle:on

  def getStatus: HealthCheckStatus = status

  def fail(e: Throwable): Unit = {
    val s = Failure(e.getClass.getName + ": " + e.getMessage, e)
    logger.warn(s"Failing HealthCheck with '${s.msg}'", e)
    status = s
  }

  val rabbitExceptionHandler = new ConnectionListener with ChannelListener with ConsumerListener {
    override def onRecoveryCompleted(connection: Connection): Unit = ()
    override def onRecoveryStarted(connection: Connection): Unit = ()
    override def onRecoveryFailure(connection: Connection, failure: Throwable): Unit = fail(failure)
    override def onRecovery(connection: Connection): Unit = ()
    override def onCreate(connection: Connection): Unit = ()
    override def onCreateFailure(failure: Throwable): Unit = fail(failure)
    override def onRecoveryCompleted(channel: Channel): Unit = ()
    override def onRecoveryStarted(channel: Channel): Unit = ()
    override def onRecoveryFailure(channel: Channel, failure: Throwable): Unit = fail(failure)
    override def onRecovery(channel: Channel): Unit = ()
    override def onCreate(channel: Channel): Unit = ()
    override def onRecoveryCompleted(consumer: Consumer, channel: Channel): Unit = ()
    override def onRecoveryStarted(consumer: Consumer, channel: Channel): Unit = ()
    override def onRecoveryFailure(consumer: Consumer, channel: Channel, failure: Throwable): Unit = fail(failure)
    override def onShutdown(consumer: Consumer, channel: Channel, consumerTag: String, sig: ShutdownSignalException): Unit = fail(sig)
    override def onShutdown(cause: ShutdownSignalException, channel: Channel): Unit = fail(cause)
    override def onError(consumer: Consumer, channel: Channel, failure: Throwable): Unit = fail(failure)
  }

}
