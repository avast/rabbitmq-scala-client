package com.avast.clients

import java.util.concurrent.Executors

import cats.arrow.FunctionK
import cats.~>
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.{RecoverableChannel, RecoverableConnection}
import mainecoon.FunctorK
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.{ExecutionContext, Future}
import scala.language.{higherKinds, implicitConversions}

package object rabbitmq {
  private[rabbitmq] type ServerConnection = RecoverableConnection
  private[rabbitmq] type ServerChannel = RecoverableChannel

  private[rabbitmq] val ses = Executors.newScheduledThreadPool(2)

  type DeliveryReadAction[F[_], -A] = Delivery[A] => F[DeliveryResult]
  type ParsingFailureAction[F[_]] = (String, Delivery[Bytes], ConversionException) => F[DeliveryResult]

  type FromTask[A[_]] = FunctionK[Task, A]
  type ToTask[A[_]] = FunctionK[A, Task]

  private[rabbitmq] implicit def fkToFuture(implicit ec: ExecutionContext): FromTask[Future] = new FunctionK[Task, Future] {
    override def apply[A](fa: Task[A]): Future[A] = fa.runAsync(Scheduler(ses, ec))
  }

  private[rabbitmq] implicit val fkFromFuture: ToTask[Future] = new FunctionK[Future, Task] {
    override def apply[A](fa: Future[A]): Task[A] = Task.fromFuture(fa)
  }

  implicit def producerFunctorK[A]: FunctorK[RabbitMQProducer[?[_], A]] = new FunctorK[RabbitMQProducer[?[_], A]] {
    override def mapK[F[_], G[_]](af: RabbitMQProducer[F, A])(fToG: ~>[F, G]): RabbitMQProducer[G, A] =
      (routingKey: String, body: A, properties: Option[MessageProperties]) => {
        fToG {
          af.send(routingKey, body, properties)
        }
      }
  }

  implicit class ConnectionOps(val connection: RabbitMQConnection[Task]) {
    // scalastyle:off
    def imapK[G[_]](implicit fromTask: FromTask[G], toTask: ToTask[G]): RabbitMQConnection[G] = {
      new RabbitMQConnection[G] {
        override def newChannel(): ServerChannel = connection.newChannel()

        override def newConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(readAction: DeliveryReadAction[G, A])(
            implicit ec: ExecutionContext): RabbitMQConsumer with AutoCloseable = connection.newConsumer(configName, monitor) {
          d: Delivery[A] =>
            readAction(d)
        }

        override def newProducer[A: ProductConverter](configName: String, monitor: Monitor): RabbitMQProducer[G, A] with AutoCloseable = {

          new RabbitMQProducer[G, A] with AutoCloseable {
            private val producer = connection.newProducer(configName, monitor)

            override def send(routingKey: String, body: A, properties: Option[MessageProperties]): G[Unit] =
              producer.send(routingKey, body, properties)

            override def close(): Unit = producer.close()
          }
        }

        override def newPullConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(
            implicit ec: ExecutionContext): RabbitMQPullConsumer[G, A] with AutoCloseable = {
          new RabbitMQPullConsumer[G, A] with AutoCloseable {
            private val consumer = connection.newPullConsumer(configName, monitor)

            override def pull(): G[PullResult[G, A]] = taskToG[G, PullResult[G, A]] {
              consumer.pull().map {
                case PullResult.Ok(deliveryWithHandle) =>
                  PullResult.Ok(new DeliveryWithHandle[G, A] {
                    override def delivery: Delivery[A] = deliveryWithHandle.delivery

                    override def handle(result: DeliveryResult): G[Unit] = taskToG[G, Unit](deliveryWithHandle.handle(result))
                  })
                case PullResult.EmptyQueue => PullResult.EmptyQueue
              }
            }

            override def close(): Unit = consumer.close()
          }
        }

        override def declareExchange(configName: String): G[Unit] = connection.declareExchange(configName)
        override def declareQueue(configName: String): G[Unit] = connection.declareQueue(configName)
        override def bindQueue(configName: String): G[Unit] = connection.bindQueue(configName)
        override def bindExchange(configName: String): G[Unit] = connection.bindExchange(configName)
        override def withChannel[A](f: ServerChannel => G[A]): G[A] = connection.withChannel(f(_))
        override def close(): Unit = connection.close()
        override def connectionListener: ConnectionListener = connection.connectionListener
        override def channelListener: ChannelListener = connection.channelListener
        override def consumerListener: ConsumerListener = connection.consumerListener
      }
    }

    private implicit def taskToG[G[_]: FromTask, A](ta: Task[A]): G[A] = implicitly[FromTask[G]].apply(ta)
    private implicit def taskFromG[G[_]: ToTask, A](ga: G[A]): Task[A] = implicitly[ToTask[G]].apply(ga)
  }

  private[rabbitmq] implicit class DeliveryOps[A](val d: Delivery[A]) extends AnyVal {
    def mapBody[B](f: A => B): Delivery[B] = d match {
      case ok: Delivery.Ok[A] => ok.copy(body = f(ok.body))
      case m: Delivery.MalformedContent => m
    }

    def flatMap[B](f: Delivery.Ok[A] => Delivery[B]): Delivery[B] = d match {
      case ok: Delivery.Ok[A] => f(ok)
      case m: Delivery.MalformedContent => m
    }
  }

  private[rabbitmq] implicit class DeliveryBytesOps(val d: Delivery[Bytes]) extends AnyVal {

    def toMalformed(ce: ConversionException): Delivery.MalformedContent = d match {
      case ok: Delivery.Ok[Bytes] => Delivery.MalformedContent(ok.body, ok.properties, ok.routingKey, ce)
      case m: Delivery.MalformedContent => m.copy(ce = ce)
    }
  }

}
