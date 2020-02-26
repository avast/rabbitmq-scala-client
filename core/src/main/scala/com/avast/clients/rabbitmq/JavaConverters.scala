package com.avast.clients.rabbitmq

import java.util.concurrent.{CompletableFuture, Executor}
import java.util.{Date, Optional}

import com.avast.clients.rabbitmq.api.{DeliveryMode, MessageProperties => ScalaProperties}
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.AMQP.BasicProperties

import scala.jdk.CollectionConverters._
import scala.concurrent._
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
}
