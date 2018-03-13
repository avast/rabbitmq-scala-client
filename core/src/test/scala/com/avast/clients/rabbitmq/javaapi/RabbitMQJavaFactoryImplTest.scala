//package com.avast.clients.rabbitmq.javaapi
//
//import org.scalatest.FunSuite
//
//import scala.util.{Failure, Try}
//
//class RabbitMQJavaFactoryImplTest extends FunSuite {
//
//  test("ThrowFailure") {
//    import RabbitMQJavaFactoryImpl._
//
//    Try(123).throwFailure()
//    Try("123").throwFailure()
//
//    assertThrows[IllegalArgumentException] {
//      Failure(new IllegalArgumentException()).throwFailure()
//    }
//
//    assertThrows[RuntimeException] {
//      Failure(new RuntimeException()).throwFailure()
//    }
//  }
//}
