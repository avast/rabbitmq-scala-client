package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.{CompletableFuture, Executor}
import java.util.{Date, Optional, function}

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.DeliveryReadAction
import com.avast.clients.rabbitmq.api.{DeliveryMode, Delivery => ScalaDelivery, DeliveryResult => ScalaResult, DeliveryWithHandle => ScalaDeliveryWithHandle, MessageProperties => ScalaProperties}
import com.avast.clients.rabbitmq.javaapi.{Delivery => JavaDelivery, DeliveryResult => JavaResult, DeliveryWithHandle => JavaDeliveryWithHandle, MessageProperties => JavaProperties}
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.AMQP.BasicProperties

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

private[rabbitmq] object JavaConverters {

  implicit class ScalaFuture2JavaCompletableFuture[A](val f: Future[A]) extends AnyVal {
    def asJava(implicit executor: ExecutionContext): CompletableFuture[A] = {
      val promise = new CompletableFuture[A]()

      f onComplete {
        case Success(result) => promise.complete(result)
        case Failure(ex) => promise.completeExceptionally(ex)
      }

      promise
    }
  }

  implicit class CompletableFuture2ScalaFuture[A](val f: CompletableFuture[A]) extends AnyVal {
    def asScala(implicit executor: Executor): Future[A] = {
      val promise = Promise[A]()

      f.whenCompleteAsync((result: A, ex: Throwable) => {
        if (ex == null) {
          promise.success(result)
        } else {
          promise.failure(ex)
        }
        ()
      }, executor)

      promise.future
    }
  }

  implicit class ScalaOption2JavaOptional[T](val o: Option[T]) extends AnyVal {
    def asJava: Optional[T] = o match {
      case Some(value) => Optional.of(value)
      case None => Optional.empty()
    }
  }

  implicit class ScalaPropertiesConversions(val messageProperties: ScalaProperties) extends AnyVal {
    def asJava: JavaProperties = {
      val builder = MessageProperties.newBuilder()

      builder.headers(messageProperties.headers.asJava)
      messageProperties.contentType.foreach(builder.contentType)
      messageProperties.contentEncoding.foreach(builder.contentEncoding)
      messageProperties.deliveryMode.foreach(dm => builder.deliveryMode(dm.code))
      messageProperties.priority.foreach(builder.priority)
      messageProperties.correlationId.foreach(builder.correlationId)
      messageProperties.replyTo.foreach(builder.replyTo)
      messageProperties.expiration.foreach(builder.expiration)
      messageProperties.messageId.foreach(builder.messageId)
      messageProperties.timestamp.map(i => new Date(i.toEpochMilli)).foreach(builder.timestamp)
      messageProperties.`type`.foreach(builder.`type`)
      messageProperties.userId.foreach(builder.userId)
      messageProperties.appId.foreach(builder.appId)
      messageProperties.clusterId.foreach(builder.clusterId)

      builder.build()
    }

    def asAMQP: AMQP.BasicProperties = {
      val builder = new BasicProperties.Builder()

      if (messageProperties.headers.nonEmpty) {
        builder.headers(messageProperties.headers.asJava)
      }
      messageProperties.contentType.foreach(builder.contentType)
      messageProperties.contentEncoding.foreach(builder.contentEncoding)
      messageProperties.deliveryMode.foreach(dm => builder.deliveryMode(dm.code))
      messageProperties.priority.foreach(builder.priority)
      messageProperties.correlationId.foreach(builder.correlationId)
      messageProperties.replyTo.foreach(builder.replyTo)
      messageProperties.expiration.foreach(builder.expiration)
      messageProperties.messageId.foreach(builder.messageId)
      messageProperties.timestamp.map(i => new Date(i.toEpochMilli)).foreach(builder.timestamp)
      messageProperties.`type`.foreach(builder.`type`)
      messageProperties.userId.foreach(builder.userId)
      messageProperties.appId.foreach(builder.appId)
      messageProperties.clusterId.foreach(builder.clusterId)

      builder.build()
    }
  }

  implicit class AmqpPropertiesConversions(val properties: AMQP.BasicProperties) extends AnyVal {
    def asScala: ScalaProperties = {
      ScalaProperties(
        Option(properties.getContentType),
        Option(properties.getContentEncoding),
        Option(properties.getHeaders).map(_.asScala.toMap).getOrElse(Map.empty),
        DeliveryMode.fromCode(properties.getDeliveryMode),
        Option(properties.getPriority),
        Option(properties.getCorrelationId),
        Option(properties.getReplyTo),
        Option(properties.getExpiration),
        Option(properties.getMessageId),
        Option(properties.getTimestamp).map(_.toInstant),
        Option(properties.getType),
        Option(properties.getUserId),
        Option(properties.getAppId),
        Option(properties.getClusterId)
      )
    }
  }

  implicit class JavaPropertiesConversions(val properties: JavaProperties) extends AnyVal {
    def asScala: ScalaProperties = {
      ScalaProperties(
        Option(properties.getContentType),
        Option(properties.getContentEncoding),
        Option(properties.getHeaders).map(_.asScala.toMap).getOrElse(Map.empty),
        DeliveryMode.fromCode(properties.getDeliveryMode),
        Option(properties.getPriority),
        Option(properties.getCorrelationId),
        Option(properties.getReplyTo),
        Option(properties.getExpiration),
        Option(properties.getMessageId),
        Option(properties.getTimestamp).map(_.toInstant),
        Option(properties.getType),
        Option(properties.getUserId),
        Option(properties.getAppId),
        Option(properties.getClusterId)
      )
    }
  }

  implicit class ScalaDeliveryConversion(val d: ScalaDelivery[Bytes]) extends AnyVal {
    def asJava: JavaDelivery = d match {
      case ScalaDelivery.Ok(body, properties, routingKey) => new JavaDelivery(routingKey, body, properties.asJava)
      case ScalaDelivery.MalformedContent(_, _, _, ce) => throw ce
    }
  }

  implicit class JavaDeliveryConversion(val d: JavaDelivery) extends AnyVal {
    def asScala: ScalaDelivery[Bytes] = {
      ScalaDelivery.Ok(d.getBody, d.getProperties.asScala, d.getRoutingKey)
    }
  }

  implicit class JavaResultConversion(val result: JavaResult) extends AnyVal {

    import ScalaResult._

    def asScala: ScalaResult = {
      result match {
        case _: JavaResult.Ack => Ack
        case _: JavaResult.Reject => Reject
        case _: JavaResult.Retry => Retry
        case r: JavaResult.Republish => Republish(r.getNewHeaders.asScala.toMap)
      }
    }
  }

  implicit class ScalaResultConversion(val result: ScalaResult) extends AnyVal {

    import ScalaResult._

    def asJava: JavaResult = {
      result match {
        case Ack => JavaResult.Ack
        case Reject => JavaResult.Reject
        case Retry => JavaResult.Retry
        case Republish(h) => JavaResult.Republish(h.asJava)
      }
    }
  }

  implicit class JavaActionConversion(val readAction: function.Function[JavaDelivery, CompletableFuture[DeliveryResult]]) extends AnyVal {
    def asScala(implicit ex: Executor, ec: ExecutionContext): DeliveryReadAction[Future, Bytes] =
      d => readAction(d.asJava).asScala.map(_.asScala)
  }

  implicit class ScalaDeliveryWithHandleConversion(val deliveryWithHandle: ScalaDeliveryWithHandle[Future, Bytes]) extends AnyVal {
    def asJava(implicit ec: ExecutionContext): JavaDeliveryWithHandle = {
      new JavaDeliveryWithHandle {
        override def delivery: JavaDelivery = deliveryWithHandle.delivery.asJava

        override def handle(result: JavaResult): CompletableFuture[Void] = {
          deliveryWithHandle.handle(result.asScala).map(_ => null: Void).asJava
        }
      }
    }
  }

}
