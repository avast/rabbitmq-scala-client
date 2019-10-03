package com.avast.clients.rabbitmq.extras

import cats.arrow.FunctionK
import cats.effect.{Effect, Resource, SyncIO}
import cats.~>
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.{
  api,
  ChannelListener,
  ConnectionListener,
  ConsumerConfig,
  ConsumerListener,
  DeliveryConverter,
  DeliveryReadAction,
  ProducerConfig,
  ProductConverter,
  PullConsumerConfig,
  RabbitMQConnection,
  ServerChannel
}
import com.avast.metrics.scalaapi.Monitor
import mainecoon.FunctorK

import scala.language.higherKinds

package object testing {

  implicit class ConnectionOps[F[_]](val conn: RabbitMQConnection[F]) {

    private implicit def resourceFToSyncIO[A](rt: Resource[F, A])(implicit F: Effect[F]): Resource[SyncIO, A] = {
      rt.mapK(fkFToSyncIO)
    }

    private implicit def fToSyncIO[A](rt: F[A])(implicit F: Effect[F]): SyncIO[A] = fkFToSyncIO.apply(rt)

    // scalastyle:off
    def asBlocking(implicit F: Effect[F]): RabbitMQConnection[SyncIO] = {
      new RabbitMQConnection[SyncIO] {
        override def newChannel(): Resource[SyncIO, ServerChannel] = conn.newChannel()

        override def newConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(
            readAction: DeliveryReadAction[SyncIO, A]): Resource[SyncIO, RabbitMQConsumer[SyncIO]] = {
          conn.newConsumer(configName, monitor)(readAction.andThen(_.to[F])).map { cons =>
            FunctorK[RabbitMQConsumer].mapK(cons)(fkFToSyncIO)
          }
        }

        override def newConsumer[A: DeliveryConverter](consumerConfig: ConsumerConfig, monitor: Monitor)(
            readAction: DeliveryReadAction[SyncIO, A]): Resource[SyncIO, RabbitMQConsumer[SyncIO]] = {
          conn.newConsumer(consumerConfig, monitor)(readAction.andThen(_.to[F])).map { cons =>
            FunctorK[RabbitMQConsumer].mapK(cons)(fkFToSyncIO)
          }
        }

        override def newProducer[A: ProductConverter](configName: String,
                                                      monitor: Monitor): Resource[SyncIO, RabbitMQProducer[SyncIO, A]] = {
          conn.newProducer(configName, monitor).map { prod =>
            FunctorK[RabbitMQProducer[*[_], A]].mapK(prod)(fkFToSyncIO)
          }
        }

        override def newProducer[A: ProductConverter](producerConfig: ProducerConfig,
                                                      monitor: Monitor): Resource[SyncIO, RabbitMQProducer[SyncIO, A]] = {
          conn.newProducer(producerConfig, monitor).map { prod =>
            FunctorK[RabbitMQProducer[*[_], A]].mapK(prod)(fkFToSyncIO)
          }
        }

        override def newPullConsumer[A: DeliveryConverter](configName: String,
                                                           monitor: Monitor): Resource[SyncIO, api.RabbitMQPullConsumer[SyncIO, A]] = {
          conn.newPullConsumer(configName, monitor).map { cons =>
            pullConsumerToSyncIO(cons)
          }
        }

        override def newPullConsumer[A: DeliveryConverter](pullConsumerConfig: PullConsumerConfig,
                                                           monitor: Monitor): Resource[SyncIO, api.RabbitMQPullConsumer[SyncIO, A]] = {
          conn.newPullConsumer(pullConsumerConfig, monitor).map { cons =>
            pullConsumerToSyncIO(cons)
          }
        }

        override def declareExchange(configName: String): SyncIO[Unit] = conn.declareExchange(configName)

        override def declareQueue(configName: String): SyncIO[Unit] = conn.declareQueue(configName)

        override def bindQueue(configName: String): SyncIO[Unit] = conn.bindQueue(configName)

        override def bindExchange(configName: String): SyncIO[Unit] = conn.bindExchange(configName)

        override def withChannel[A](f: ServerChannel => SyncIO[A]): SyncIO[A] = conn.withChannel(f(_).to[F])

        override val connectionListener: ConnectionListener = conn.connectionListener

        override val channelListener: ChannelListener = conn.channelListener

        override val consumerListener: ConsumerListener = conn.consumerListener
      }
    }
  }

  private def fkFToSyncIO[F[_]](implicit F: Effect[F]): FunctionK[F, SyncIO] = new FunctionK[F, SyncIO] {
    override def apply[A](fa: F[A]): SyncIO[A] = SyncIO.apply(F.toIO(fa).unsafeRunSync())
  }

  private def pullConsumerToSyncIO[F[_], A: DeliveryConverter](cons: RabbitMQPullConsumer[F, A])(
      implicit F: Effect[F]): RabbitMQPullConsumer[SyncIO, A] = { () =>
    fkFToSyncIO.apply(cons.pull()).map {
      case ok: PullResult.Ok[F, A] =>
        import ok._ // see https://users.scala-lang.org/t/constructor-pattern-match-constructor-cannot-be-instantiated-to-expected-type/435

        PullResult.Ok(new DeliveryWithHandle[SyncIO, A] {
          override def delivery: Delivery[A] = deliveryWithHandle.delivery

          override def handle(result: DeliveryResult): SyncIO[Unit] = fkFToSyncIO.apply(deliveryWithHandle.handle(result))
        })

      case PullResult.EmptyQueue => PullResult.EmptyQueue
    }
  }

  private implicit def producerFunctorK[A]: FunctorK[RabbitMQProducer[*[_], A]] = new FunctorK[RabbitMQProducer[*[_], A]] {
    override def mapK[F[_], G[_]](af: RabbitMQProducer[F, A])(fToG: ~>[F, G]): RabbitMQProducer[G, A] =
      (routingKey: String, body: A, properties: Option[MessageProperties]) => {
        fToG {
          af.send(routingKey, body, properties)
        }
      }
  }

  private implicit val consumerFunctorK: FunctorK[RabbitMQConsumer] = new FunctorK[RabbitMQConsumer] {
    override def mapK[F[_], G[_]](af: RabbitMQConsumer[F])(fk: F ~> G): RabbitMQConsumer[G] = {
      new RabbitMQConsumer[G] {} // no-op
    }
  }

}
