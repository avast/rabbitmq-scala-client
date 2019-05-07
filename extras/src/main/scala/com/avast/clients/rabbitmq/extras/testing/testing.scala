package com.avast.clients.rabbitmq.extras

import cats.arrow.FunctionK
import cats.effect.Resource
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
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

package object testing {

  implicit class ConnectionOps(val conn: RabbitMQConnection[Task]) {

    private implicit def resourceTaskToCoeval[A](rt: Resource[Task, A])(implicit sch: Scheduler): Resource[Coeval, A] = {
      rt.mapK(fkTaskToCoeval)
    }

    private implicit def taskToCoeval[A](rt: Task[A])(implicit sch: Scheduler): Coeval[A] = fkTaskToCoeval.apply(rt)

    // scalastyle:off
    def asBlocking(implicit sch: Scheduler): RabbitMQConnection[Coeval] = {
      new RabbitMQConnection[Coeval] {
        override def newChannel(): Resource[Coeval, ServerChannel] = conn.newChannel()

        override def newConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(readAction: DeliveryReadAction[Coeval, A])(
            implicit ec: ExecutionContext): Resource[Coeval, RabbitMQConsumer[Coeval]] = {
          conn.newConsumer(configName, monitor)(readAction.andThen(_.task)).map { cons =>
            FunctorK[RabbitMQConsumer].mapK(cons)(fkTaskToCoeval)
          }
        }

        override def newConsumer[A: DeliveryConverter](consumerConfig: ConsumerConfig, monitor: Monitor)(
            readAction: DeliveryReadAction[Coeval, A])(implicit ec: ExecutionContext): Resource[Coeval, RabbitMQConsumer[Coeval]] = {
          conn.newConsumer(consumerConfig, monitor)(readAction.andThen(_.task)).map { cons =>
            FunctorK[RabbitMQConsumer].mapK(cons)(fkTaskToCoeval)
          }
        }

        override def newProducer[A: ProductConverter](configName: String, monitor: Monitor)(
            implicit ec: ExecutionContext): Resource[Coeval, RabbitMQProducer[Coeval, A]] = {
          conn.newProducer(configName, monitor).map { prod =>
            FunctorK[RabbitMQProducer[?[_], A]].mapK(prod)(fkTaskToCoeval)
          }
        }

        override def newProducer[A: ProductConverter](producerConfig: ProducerConfig, monitor: Monitor)(
            implicit ec: ExecutionContext): Resource[Coeval, RabbitMQProducer[Coeval, A]] = {
          conn.newProducer(producerConfig, monitor).map { prod =>
            FunctorK[RabbitMQProducer[?[_], A]].mapK(prod)(fkTaskToCoeval)
          }
        }

        override def newPullConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(
            implicit ec: ExecutionContext): Resource[Coeval, api.RabbitMQPullConsumer[Coeval, A]] = {
          conn.newPullConsumer(configName, monitor).map { cons =>
            pullConsumerToCoeval(cons)
          }
        }

        override def newPullConsumer[A: DeliveryConverter](pullConsumerConfig: PullConsumerConfig, monitor: Monitor)(
            implicit ec: ExecutionContext): Resource[Coeval, api.RabbitMQPullConsumer[Coeval, A]] = {
          conn.newPullConsumer(pullConsumerConfig, monitor).map { cons =>
            pullConsumerToCoeval(cons)
          }
        }

        override def declareExchange(configName: String): Coeval[Unit] = conn.declareExchange(configName)

        override def declareQueue(configName: String): Coeval[Unit] = conn.declareQueue(configName)

        override def bindQueue(configName: String): Coeval[Unit] = conn.bindQueue(configName)

        override def bindExchange(configName: String): Coeval[Unit] = conn.bindExchange(configName)

        override def withChannel[A](f: ServerChannel => Coeval[A]): Coeval[A] = conn.withChannel(f(_).task)

        override val connectionListener: ConnectionListener = conn.connectionListener

        override val channelListener: ChannelListener = conn.channelListener

        override val consumerListener: ConsumerListener = conn.consumerListener
      }
    }
  }

  private def fkTaskToCoeval(implicit sch: Scheduler): FunctionK[Task, Coeval] = new FunctionK[Task, Coeval] {
    override def apply[A](fa: Task[A]): Coeval[A] = Coeval(fa.runSyncUnsafe())
  }

  private def pullConsumerToCoeval[A: DeliveryConverter](cons: RabbitMQPullConsumer[Task, A])(
      implicit sch: Scheduler): RabbitMQPullConsumer[Coeval, A] = { () =>
    fkTaskToCoeval.apply(cons.pull()).map {
      case PullResult.Ok(deliveryWithHandle) =>
        PullResult.Ok(new DeliveryWithHandle[Coeval, A] {
          override def delivery: Delivery[A] = deliveryWithHandle.delivery

          override def handle(result: DeliveryResult): Coeval[Unit] = fkTaskToCoeval.apply(deliveryWithHandle.handle(result))
        })
      case PullResult.EmptyQueue => PullResult.EmptyQueue
    }
  }

  private implicit def producerFunctorK[A]: FunctorK[RabbitMQProducer[?[_], A]] = new FunctorK[RabbitMQProducer[?[_], A]] {
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
