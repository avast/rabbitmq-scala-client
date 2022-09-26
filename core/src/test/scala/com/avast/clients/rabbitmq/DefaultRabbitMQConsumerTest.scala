package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.DefaultRabbitMQConsumer.CorrelationIdHeaderName
import com.avast.clients.rabbitmq.RabbitMQConnection.DefaultListeners
import com.avast.clients.rabbitmq.api.DeliveryResult
import com.avast.clients.rabbitmq.api.DeliveryResult.Republish
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaeffectapi._
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Matchers}
import org.scalatest.time.{Seconds, Span}
import org.slf4j.event.Level

import java.time.Duration
import java.util.UUID
import scala.collection.immutable
import scala.concurrent.duration.{DurationInt, Duration => ScalaDuration}
import scala.jdk.CollectionConverters._
import scala.util._

class DefaultRabbitMQConsumerTest extends TestBase {

  private val connectionInfo = RabbitMQConnectionInfo(immutable.Seq("localhost"), "/", None)

  test("should ACK") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val consumer = newConsumer(channel)({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      Task.now(DeliveryResult.Ack)
    })

    val body = Random.nextString(5).getBytes
    consumer.handleDelivery("abcd", envelope, properties, body)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(1)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicReject(deliveryTag, true)
      verify(channel, times(0)).basicReject(deliveryTag, false)
      verify(channel, times(0)).basicPublish("", "queueName", properties, body)
    }
  }

  test("should RETRY") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val consumer = newConsumer(channel)({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      Task.now(DeliveryResult.Retry)
    })

    val body = Random.nextString(5).getBytes
    consumer.handleDelivery("abcd", envelope, properties, body)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(1)).basicReject(deliveryTag, true)
      verify(channel, times(0)).basicReject(deliveryTag, false)
      verify(channel, times(0)).basicPublish("", "queueName", properties, body)
    }
  }

  test("should REJECT") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val consumer = newConsumer(channel)({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      Task.now(DeliveryResult.Reject)
    })

    val body = Random.nextString(5).getBytes
    consumer.handleDelivery("abcd", envelope, properties, body)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicReject(deliveryTag, true)
      verify(channel, times(1)).basicReject(deliveryTag, false)
      verify(channel, times(0)).basicPublish("", "queueName", properties, body)
    }
  }

  test("should REPUBLISH") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val originalUserId = "OriginalUserId"
    val properties = new BasicProperties.Builder().messageId(messageId).userId(originalUserId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val consumer = newConsumer(channel)({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      Task.now(DeliveryResult.Republish())
    })

    val body = Random.nextString(5).getBytes
    consumer.handleDelivery("abcd", envelope, properties, body)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(1)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicReject(deliveryTag, true)
      verify(channel, times(0)).basicReject(deliveryTag, false)
      val propertiesCaptor = ArgumentCaptor.forClass(classOf[BasicProperties])
      verify(channel, times(1)).basicPublish(Matchers.eq(""), Matchers.eq("queueName"), propertiesCaptor.capture(), Matchers.eq(body))
      assertResult(Some(originalUserId))(propertiesCaptor.getValue.getHeaders.asScala.get(DefaultRabbitMQConsumer.RepublishOriginalUserId))
    }
  }

  test("should NACK because of failure") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val consumer = newConsumer(channel, failureAction = DeliveryResult.Retry)({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      Task.raiseError(new RuntimeException)
    })

    consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(1)).basicReject(deliveryTag, true)
    }
  }

  test("should NACK because of unexpected failure") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val consumer = newConsumer(channel, failureAction = DeliveryResult.Retry)({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      throw new RuntimeException
    })

    consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(1)).basicReject(deliveryTag, true)
    }
  }

  test("measures processed time correctly - success") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val monitor = mock[Monitor[Task]]
    when(monitor.meter(Matchers.anyString())).thenReturn(Monitor.noOp[Task]().meter(""))
    when(monitor.named(Matchers.eq("results"))).thenReturn(Monitor.noOp[Task]())
    val tasksMonitor = mock[Monitor[Task]]
    when(monitor.named(Matchers.eq("tasks"))).thenReturn(tasksMonitor)
    when(tasksMonitor.gauge).thenReturn(new GaugeFactory[Task] {
      override def settableLong(n: String, replaceExisting: Boolean): SettableGauge[Task, Long] = new SettableGauge[Task, Long] {
        override def set(value: Long): Task[Unit] = fail("Should have not be called")
        override def update(f: Long => Long): Task[Long] = fail("Should have not be called")
        override def inc: Task[Long] = Task.now(42) // returned value is not used anywhere
        override def dec: Task[Long] = Task.now(42) // returned value is not used anywhere
        override def value: Task[Long] = fail("Should have not be called")
        override def name: String = n
      }
      override def settableDouble(name: String, replaceExisting: Boolean): SettableGauge[Task, Double] = fail("Should have not be called")
      override def generic[T](name: String, replaceExisting: Boolean)(gauge: () => T): Gauge[Task, T] = fail("Should have not be called")
    })

    var successLengths = Seq.newBuilder[Long] // scalastyle:ignore
    var failuresLengths = Seq.newBuilder[Long] // scalastyle:ignore

    when(tasksMonitor.timerPair(Matchers.eq("processed"))).thenReturn(new TimerPair[Task] {
      override def update(duration: Duration): Task[Unit] = Task.delay(successLengths += duration.toMillis)
      override def updateFailure(duration: Duration): Task[Unit] = Task.delay(failuresLengths += duration.toMillis)

      override def update(duration: ScalaDuration): Task[Unit] = fail("Should have not be called")
      override def updateFailure(duration: ScalaDuration): Task[Unit] = fail("Should have not be called")
      override def start(): Task[TimerPairContext] = fail("Should have not be called")
      override def time[T](action: Task[T]): Task[T] = fail("Should have not be called")
      override def time[T](action: Task[T])(successCheck: T => Boolean): Task[T] = fail("Should have not be called")
    })

    {
      val consumer = newConsumer(channel, DeliveryResult.Retry, monitor)({ delivery =>
        assertResult(Some(messageId))(delivery.properties.messageId)
        Task.now(DeliveryResult.Ack) // immediate
      })

      consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

      eventually(timeout(Span(3, Seconds)), interval(Span(0.1, Seconds))) {
        assertResult(Seq.empty)(failuresLengths)
        val Seq(taskLength) = successLengths.result()

        assert(taskLength < 200)
      }
    }

    successLengths = Seq.newBuilder
    failuresLengths = Seq.newBuilder

    {
      val consumer = newConsumer(channel, DeliveryResult.Retry, monitor)({ delivery =>
        assertResult(Some(messageId))(delivery.properties.messageId)
        import scala.concurrent.duration._
        Task.now(DeliveryResult.Ack).delayResult(2.second)
      })

      consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

      eventually(timeout(Span(3, Seconds)), interval(Span(0.1, Seconds))) {
        assertResult(Seq.empty)(failuresLengths)
        val Seq(taskLength) = successLengths.result()

        assert(taskLength > 1990) // 2000 minus some tolerance
      }
    }
  }

  test("measures processed time correctly - failure") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val monitor = mock[Monitor[Task]]
    when(monitor.meter(Matchers.anyString())).thenReturn(Monitor.noOp[Task]().meter(""))
    when(monitor.named(Matchers.eq("results"))).thenReturn(Monitor.noOp[Task]())
    val tasksMonitor = mock[Monitor[Task]]
    when(monitor.named(Matchers.eq("tasks"))).thenReturn(tasksMonitor)
    when(tasksMonitor.gauge).thenReturn(new GaugeFactory[Task] {
      override def settableLong(n: String, replaceExisting: Boolean): SettableGauge[Task, Long] = new SettableGauge[Task, Long] {
        override def set(value: Long): Task[Unit] = fail("Should have not be called")
        override def update(f: Long => Long): Task[Long] = fail("Should have not be called")
        override def inc: Task[Long] = Task.now(42) // returned value is not used anywhere
        override def dec: Task[Long] = Task.now(42) // returned value is not used anywhere
        override def value: Task[Long] = fail("Should have not be called")
        override def name: String = n
      }
      override def settableDouble(name: String, replaceExisting: Boolean): SettableGauge[Task, Double] = fail("Should have not be called")
      override def generic[T](name: String, replaceExisting: Boolean)(gauge: () => T): Gauge[Task, T] = fail("Should have not be called")
    })

    var successLengths = Seq.newBuilder[Long] // scalastyle:ignore
    var failuresLengths = Seq.newBuilder[Long] // scalastyle:ignore

    when(tasksMonitor.timerPair(Matchers.eq("processed"))).thenReturn(new TimerPair[Task] {
      override def update(duration: Duration): Task[Unit] = Task.delay(successLengths += duration.toMillis)
      override def updateFailure(duration: Duration): Task[Unit] = Task.delay(failuresLengths += duration.toMillis)

      override def update(duration: ScalaDuration): Task[Unit] = fail("Should have not be called")
      override def updateFailure(duration: ScalaDuration): Task[Unit] = fail("Should have not be called")
      override def start(): Task[TimerPairContext] = fail("Should have not be called")
      override def time[T](action: Task[T]): Task[T] = fail("Should have not be called")
      override def time[T](action: Task[T])(successCheck: T => Boolean): Task[T] = fail("Should have not be called")
    })

    {
      val consumer = newConsumer(channel, DeliveryResult.Retry, monitor)({ delivery =>
        assertResult(Some(messageId))(delivery.properties.messageId)
        Task.raiseError(new RuntimeException) // immediate
      })

      consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

      eventually(timeout(Span(3, Seconds)), interval(Span(0.1, Seconds))) {
        assertResult(Seq.empty)(successLengths)
        val Seq(taskLength) = failuresLengths.result()

        assert(taskLength < 200)
      }
    }

    successLengths = Seq.newBuilder
    failuresLengths = Seq.newBuilder

    {
      val consumer = newConsumer(channel, DeliveryResult.Retry, monitor)({ delivery =>
        assertResult(Some(messageId))(delivery.properties.messageId)
        import scala.concurrent.duration._
        Task.raiseError(new RuntimeException("my exception")).delayExecution(2.second)
      })

      consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

      eventually(timeout(Span(3, Seconds)), interval(Span(0.1, Seconds))) {
        assertResult(Seq.empty)(successLengths)
        val Seq(taskLength) = failuresLengths.result()

        assert(taskLength > 1990) // 2000 minus some tolerance
      }
    }
  }

  test("passes correlation id") {
    val messageId = UUID.randomUUID().toString
    val correlationId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder().messageId(messageId).correlationId(correlationId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val consumer = newConsumer(channel, failureAction = DeliveryResult.Reject)({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)
      assertResult(Some(correlationId))(delivery.properties.correlationId)

      Task.now(DeliveryResult.Ack)
    })

    val body = Random.nextString(5).getBytes
    consumer.handleDelivery("abcd", envelope, properties, body)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(1)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicReject(deliveryTag, false)
    }
  }

  test("parses correlation id from header") {
    val messageId = UUID.randomUUID().toString
    val correlationId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder()
      .messageId(messageId)
      .headers(Map(CorrelationIdHeaderName -> correlationId.asInstanceOf[AnyRef]).asJava)
      .build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val consumer = newConsumer(channel, failureAction = DeliveryResult.Reject) { delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)
      assertResult(Some(correlationId))(delivery.properties.correlationId)

      Task.now(DeliveryResult.Ack)
    }

    val body = Random.nextString(5).getBytes
    consumer.handleDelivery("abcd", envelope, properties, body)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(1)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicReject(deliveryTag, false)
    }
  }

  private def newConsumer(channel: ServerChannel, failureAction: DeliveryResult = Republish(), monitor: Monitor[Task] = Monitor.noOp())(
      userAction: DeliveryReadAction[Task, Bytes]): DefaultRabbitMQConsumer[Task, Bytes] = {
    val base = new ConsumerBase[Task, Bytes](
      "test",
      "queueName",
      false,
      TestBase.testBlocker,
      ImplicitContextLogger.createLogger,
      monitor
    )

    val channelOps = new ConsumerChannelOps[Task, Bytes](
      "test",
      "queueName",
      channel,
      TestBase.testBlocker,
      RepublishStrategy.DefaultExchange[Task](),
      PMH,
      connectionInfo,
      ImplicitContextLogger.createLogger,
      monitor
    )

    new DefaultRabbitMQConsumer[Task, Bytes](
      base,
      channelOps,
      10.seconds,
      DeliveryResult.Republish(),
      Level.ERROR,
      failureAction,
      DefaultListeners.defaultConsumerListener,
    )(userAction)
  }

  object PMH extends LoggingPoisonedMessageHandler[Task, Bytes](3, false, None)
}
