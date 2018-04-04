package com.avast.clients.rabbitmq.ssl

import java.io.InputStream
import java.security.KeyStore
import javax.net.ssl.{KeyManagerFactory, TrustManagerFactory, X509KeyManager, X509TrustManager}

/**
  * Utils for keystore bundle opening.
  */
object KeyStoreUtils {

  /**
    * Opens given file as keystore of given type.
    *
    * @param inputStream  InputStream to be loaded as keystore.
    * @param keyStoreType Type of the Keystore.
    * @return Opened keystore.
    */
  def openKeyStore(inputStream: InputStream, keyStorePassword: String, keyStoreType: KeyStoreType): OpenedKeyStore = {
    require(inputStream != null, "Given inputStream must not be NULL")

    val ks = KeyStore.getInstance(keyStoreType.keyStoreType)
    ks.load(inputStream, keyStorePassword.toCharArray)

    val keyManagers = keyStoreType match {
      case k: KeyStoreTypes.KeyStore => getKeyManagers(ks, k.keysPassword)
      case _ => Seq()
    }

    val trustManagers = keyStoreType match {
      case k: KeyStoreTypes.TrustStore => getTrustManagers(ks)
      case _ => Seq()
    }

    OpenedKeyStore(keyManagers, trustManagers)
  }

  def getKeyManagers(keystore: KeyStore, keyPassword: String): Seq[X509KeyManager] = {
    val factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    factory.init(keystore, Option(keyPassword).map(_.toCharArray).orNull)

    factory.getKeyManagers.toSeq
      .filter(_.isInstanceOf[X509KeyManager])
      .map(_.asInstanceOf[X509KeyManager])
  }

  final lazy val SystemKeyManagers: Seq[X509KeyManager] = {
    getKeyManagers(null, null)
  }

  def getTrustManagers(keystore: KeyStore): Seq[X509TrustManager] = {
    val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    factory.init(keystore)

    factory.getTrustManagers.toSeq
      .filter(_.isInstanceOf[X509TrustManager])
      .map(_.asInstanceOf[X509TrustManager])
  }

  final lazy val SystemTrustManagers: Seq[X509TrustManager] = getTrustManagers(null)
}

/**
  * Holder for keyManagers and trustManagers loaded from keystore.
  */
case class OpenedKeyStore(keyManagers: Seq[X509KeyManager], trustManagers: Seq[X509TrustManager])
