package com.avast.clients.rabbitmq.extras.pureconfig

import cats.effect.{Effect, Resource, SyncIO}
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.pureconfig.PureconfigRabbitMQConnection
import com.avast.clients.rabbitmq.{
  api,
  ChannelListener,
  ConnectionListener,
  ConsumerListener,
  DeliveryConverter,
  DeliveryReadAction,
  ProductConverter,
  ServerChannel
}
import com.avast.metrics.scalaapi.Monitor
import mainecoon.FunctorK

import scala.language.higherKinds

package object testing {
  import com.avast.clients.rabbitmq.extras.testing._

  implicit class PureconfigConnectionOps[F[_]](val conn: PureconfigRabbitMQConnection[F]) {

    private implicit def resourceFToSyncIO[A](rt: Resource[F, A])(implicit F: Effect[F]): Resource[SyncIO, A] = {
      rt.mapK(fkFToSyncIO)
    }

    private implicit def fToSyncIO[A](rt: F[A])(implicit F: Effect[F]): SyncIO[A] = fkFToSyncIO.apply(rt)

    // scalastyle:off
    def asBlocking(implicit F: Effect[F]): PureconfigRabbitMQConnection[SyncIO] = {
      new PureconfigRabbitMQConnection[SyncIO] {
        override def newChannel(): Resource[SyncIO, ServerChannel] = conn.newChannel()

        override def newConsumer[A: DeliveryConverter](name: String, monitor: Monitor)(
            readAction: DeliveryReadAction[SyncIO, A]): Resource[SyncIO, RabbitMQConsumer[SyncIO]] = {
          conn.newConsumer(name, monitor)(readAction.andThen(_.to[F])).map { cons =>
            FunctorK[RabbitMQConsumer].mapK(cons)(fkFToSyncIO)
          }
        }

        override def newProducer[A: ProductConverter](name: String, monitor: Monitor): Resource[SyncIO, RabbitMQProducer[SyncIO, A]] = {
          conn.newProducer(name, monitor).map { prod =>
            FunctorK[RabbitMQProducer[*[_], A]].mapK(prod)(fkFToSyncIO)
          }
        }

        override def newPullConsumer[A: DeliveryConverter](name: String,
                                                           monitor: Monitor): Resource[SyncIO, api.RabbitMQPullConsumer[SyncIO, A]] = {
          conn.newPullConsumer(name, monitor).map { cons =>
            pullConsumerToSyncIO(cons)
          }
        }

        override def declareExchange(name: String): SyncIO[Unit] = conn.declareExchange(name)

        override def declareQueue(name: String): SyncIO[Unit] = conn.declareQueue(name)

        override def bindExchange(name: String): SyncIO[Unit] = conn.bindExchange(name)

        override def bindQueue(name: String): SyncIO[Unit] = conn.bindQueue(name)

        override def withChannel[A](f: ServerChannel => SyncIO[A]): SyncIO[A] = conn.withChannel(f(_).to[F])

        override val connectionListener: ConnectionListener = conn.connectionListener

        override val channelListener: ChannelListener = conn.channelListener

        override val consumerListener: ConsumerListener = conn.consumerListener
      }
    }
  }
}
