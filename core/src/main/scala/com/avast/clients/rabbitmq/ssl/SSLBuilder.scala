package com.avast.clients.rabbitmq.ssl

import java.io.InputStream
import java.nio.file.{Files, Path}

import com.typesafe.config.ConfigFactory
import javax.net.ssl._

import scala.collection.immutable

/**
  * Builder for SSLContext. Use static methods for it's creation.
  */
case class SSLBuilder private[ssl] (protocol: String,
                                    keyManagers: immutable.Seq[X509KeyManager] = immutable.Seq(),
                                    trustManagers: immutable.Seq[X509TrustManager] = immutable.Seq()) {

  import KeyStoreUtils._
  import SSLBuilder._

  def withKeyManagers(managers: Seq[X509KeyManager]): SSLBuilder = {
    copy(keyManagers = keyManagers ++ managers)
  }

  def withTrustManagers(managers: Seq[X509TrustManager]): SSLBuilder = {
    copy(trustManagers = trustManagers ++ managers)
  }

  def withProtocol(protocol: String): SSLBuilder = {
    copy(protocol = protocol)
  }

  def loadAllFromBundle(path: Path, keyStoreType: KeyStoreType, password: String = DefaultAvastJksPassword): SSLBuilder = {
    loadAllFromBundleStream(Files.newInputStream(path), keyStoreType, password)
  }

  def loadAllFromBundleStream(is: InputStream, keyStoreType: KeyStoreType, password: String = DefaultAvastJksPassword): SSLBuilder = {
    val OpenedKeyStore(newKeyManagers, newTrustManagers) = openKeyStore(is, password, keyStoreType)

    copy(
      keyManagers = keyManagers ++ newKeyManagers,
      trustManagers = trustManagers ++ newTrustManagers
    )
  }

  def build: SSLContext = {
    val context = SSLContext.getInstance(protocol)

    context.init(
      (keyManagers ++ SystemKeyManagers).toArray,
      Array(new CompositeX509TrustManager(trustManagers ++ SystemTrustManagers)),
      null
    )

    context
  }
}

private[rabbitmq] object SSLBuilder {

  final val DefaultAvastJksPassword = "CanNotMakeJKSWithoutPass"

  private final val RootConfigKey = "avastRabbitMQSslBuilderDefaults"

  private lazy val DefaultConfig = ConfigFactory.defaultReference().getConfig(RootConfigKey)

  def empty(): SSLBuilder = new SSLBuilder("TLS", immutable.Seq(), immutable.Seq())

  def fromBundle(path: Path, keyStoreType: KeyStoreType, password: String = DefaultAvastJksPassword): SSLBuilder = {
    fromBundleStream(Files.newInputStream(path), keyStoreType, password)
  }

  def fromBundleStream(is: InputStream, keyStoreType: KeyStoreType, password: String = DefaultAvastJksPassword): SSLBuilder = {
    empty().loadAllFromBundleStream(is, keyStoreType, password)
  }
}
