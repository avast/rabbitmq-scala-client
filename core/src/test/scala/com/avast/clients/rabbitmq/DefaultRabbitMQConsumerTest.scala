package com.avast.clients.rabbitmq

import java.time.Duration
import java.util.UUID

import com.avast.clients.rabbitmq.RabbitMQConnection.DefaultListeners
import com.avast.clients.rabbitmq.api.DeliveryResult
import com.avast.metrics.scalaapi._
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Matchers}
import org.scalatest.time.{Seconds, Span}

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.concurrent.{ExecutionContext, Future}
import scala.util._

class DefaultRabbitMQConsumerTest extends TestBase {

  private val connectionInfo = RabbitMQConnectionInfo(immutable.Seq("localhost"), "/", None)

  test("should ACK") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val consumer = new DefaultRabbitMQConsumer[Task](
      "test",
      channel,
      "queueName",
      connectionInfo,
      Monitor.noOp,
      DeliveryResult.Reject,
      DefaultListeners.DefaultConsumerListener,
      TestBase.testBlocker
    )({ delivery =>
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

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val consumer = new DefaultRabbitMQConsumer[Task](
      "test",
      channel,
      "queueName",
      connectionInfo,
      Monitor.noOp,
      DeliveryResult.Reject,
      DefaultListeners.DefaultConsumerListener,
      TestBase.testBlocker
    )({ delivery =>
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

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val consumer = new DefaultRabbitMQConsumer[Task](
      "test",
      channel,
      "queueName",
      connectionInfo,
      Monitor.noOp,
      DeliveryResult.Reject,
      DefaultListeners.DefaultConsumerListener,
      TestBase.testBlocker
    )({ delivery =>
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

    val consumer = new DefaultRabbitMQConsumer[Task](
      "test",
      channel,
      "queueName",
      connectionInfo,
      Monitor.noOp,
      DeliveryResult.Reject,
      DefaultListeners.DefaultConsumerListener,
      TestBase.testBlocker
    )({ delivery =>
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

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val consumer = new DefaultRabbitMQConsumer[Task](
      "test",
      channel,
      "queueName",
      connectionInfo,
      Monitor.noOp,
      DeliveryResult.Retry,
      DefaultListeners.DefaultConsumerListener,
      TestBase.testBlocker
    )({ delivery =>
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

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val consumer = new DefaultRabbitMQConsumer[Task](
      "test",
      channel,
      "queueName",
      connectionInfo,
      Monitor.noOp,
      DeliveryResult.Retry,
      DefaultListeners.DefaultConsumerListener,
      TestBase.testBlocker
    )({ delivery =>
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

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val monitor = mock[Monitor]
    when(monitor.meter(Matchers.anyString())).thenReturn(Monitor.noOp.meter(""))
    when(monitor.named(Matchers.eq("results"))).thenReturn(Monitor.noOp())
    val tasksMonitor = mock[Monitor]
    when(monitor.named(Matchers.eq("tasks"))).thenReturn(tasksMonitor)
    when(tasksMonitor.gauge(Matchers.anyString())(Matchers.any()))
      .thenReturn(Monitor.noOp().gauge("")(() => 0).asInstanceOf[Gauge[Nothing]])

    var successLengths = mutable.Seq.empty[Long] // scalastyle:ignore
    var failuresLengths = mutable.Seq.empty[Long] // scalastyle:ignore

    when(tasksMonitor.timerPair(Matchers.eq("processed"))).thenReturn(new TimerPair {
      override def start(): TimeContext = ???
      override def update(duration: Duration): Unit = ???

      override def updateFailure(duration: Duration): Unit = ???
      override def time[A](block: => A): A = ???
      override def time[A](future: => Future[A])(implicit ec: ExecutionContext): Future[A] = {
        val start = System.currentTimeMillis()

        future.andThen {
          case Success(_) => successLengths = successLengths :+ System.currentTimeMillis() - start
          case Failure(_) => failuresLengths = failuresLengths :+ System.currentTimeMillis() - start
        }
      }
    })

    {
      val consumer = new DefaultRabbitMQConsumer[Task](
        "test",
        channel,
        "queueName",
        connectionInfo,
        monitor,
        DeliveryResult.Retry,
        DefaultListeners.DefaultConsumerListener,
        TestBase.testBlocker
      )({ delivery =>
        assertResult(Some(messageId))(delivery.properties.messageId)
        Task.now(DeliveryResult.Ack) // immediate
      })

      consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

      eventually(timeout(Span(3, Seconds)), interval(Span(0.1, Seconds))) {
        assertResult(Seq.empty)(failuresLengths)
        val Seq(taskLength) = successLengths

        assert(taskLength < 200)
      }
    }

    successLengths = mutable.Seq.empty
    failuresLengths = mutable.Seq.empty

    {
      val consumer = new DefaultRabbitMQConsumer[Task](
        "test",
        channel,
        "queueName",
        connectionInfo,
        monitor,
        DeliveryResult.Retry,
        DefaultListeners.DefaultConsumerListener,
        TestBase.testBlocker
      )({ delivery =>
        assertResult(Some(messageId))(delivery.properties.messageId)
        import scala.concurrent.duration._
        Task.now(DeliveryResult.Ack).delayResult(2.second)
      })

      consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

      eventually(timeout(Span(3, Seconds)), interval(Span(0.1, Seconds))) {
        assertResult(Seq.empty)(failuresLengths)
        val Seq(taskLength) = successLengths

        assert(taskLength > 2000)
      }
    }
  }

  test("measures processed time correctly - failure") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val monitor = mock[Monitor]
    when(monitor.meter(Matchers.anyString())).thenReturn(Monitor.noOp.meter(""))
    when(monitor.named(Matchers.eq("results"))).thenReturn(Monitor.noOp())
    val tasksMonitor = mock[Monitor]
    when(monitor.named(Matchers.eq("tasks"))).thenReturn(tasksMonitor)
    when(tasksMonitor.gauge(Matchers.anyString())(Matchers.any()))
      .thenReturn(Monitor.noOp().gauge("")(() => 0).asInstanceOf[Gauge[Nothing]])

    var successLengths = mutable.Seq.empty[Long] // scalastyle:ignore
    var failuresLengths = mutable.Seq.empty[Long] // scalastyle:ignore

    when(tasksMonitor.timerPair(Matchers.eq("processed"))).thenReturn(new TimerPair {
      override def start(): TimeContext = ???
      override def update(duration: Duration): Unit = ???

      override def updateFailure(duration: Duration): Unit = ???
      override def time[A](block: => A): A = ???
      override def time[A](future: => Future[A])(implicit ec: ExecutionContext): Future[A] = {
        val start = System.currentTimeMillis()

        future.andThen {
          case Success(_) => successLengths = successLengths :+ System.currentTimeMillis() - start
          case Failure(_) => failuresLengths = failuresLengths :+ System.currentTimeMillis() - start
        }
      }
    })

    {
      val consumer = new DefaultRabbitMQConsumer[Task](
        "test",
        channel,
        "queueName",
        connectionInfo,
        monitor,
        DeliveryResult.Retry,
        DefaultListeners.DefaultConsumerListener,
        TestBase.testBlocker
      )({ delivery =>
        assertResult(Some(messageId))(delivery.properties.messageId)
        Task.raiseError(new RuntimeException) // immediate
      })

      consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

      eventually(timeout(Span(3, Seconds)), interval(Span(0.1, Seconds))) {
        assertResult(Seq.empty)(successLengths)
        val Seq(taskLength) = failuresLengths

        assert(taskLength < 200)
      }
    }

    successLengths = mutable.Seq.empty
    failuresLengths = mutable.Seq.empty

    {
      val consumer = new DefaultRabbitMQConsumer[Task](
        "test",
        channel,
        "queueName",
        connectionInfo,
        monitor,
        DeliveryResult.Retry,
        DefaultListeners.DefaultConsumerListener,
        TestBase.testBlocker
      )({ delivery =>
        assertResult(Some(messageId))(delivery.properties.messageId)
        import scala.concurrent.duration._
        Task.raiseError(new RuntimeException).delayExecution(2.second)
      })

      consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

      eventually(timeout(Span(3, Seconds)), interval(Span(0.1, Seconds))) {
        assertResult(Seq.empty)(successLengths)
        val Seq(taskLength) = failuresLengths

        assert(taskLength > 2000)
      }
    }
  }

}
