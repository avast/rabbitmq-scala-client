package com.avast.clients.rabbitmq.ssl

import java.nio.file.{Files, Path, Paths}

import com.avast.clients.rabbitmq.ssl.KeyStoreTypes.{JKSKeyStore, JKSTrustStore, PKCS12KeyStore, PKCS12TrustStore}
import com.typesafe.config.Config
import javax.net.ssl.{X509KeyManager, X509TrustManager}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

private[ssl] object SSLBuilderValueReader extends ValueReader[SSLBuilder] {

  object SslContextConfigKeys {
    final val Protocol = "protocol"

    object General {
      final val `Type` = "type"
      final val Path = "path"
      final val Password = "password"
      final val KeyPassword = "keyPassword"
    }

    object KeyStores {
      final val root = "keyStores"
    }

    object KeyStoresAndTrustStores {
      final val root = "keyStoresAndTrustStores"
    }

    object TrustStores {
      final val root = "trustStores"
    }

  }

  import SslContextConfigKeys._

  override def read(rootConfig: Config, path: String): SSLBuilder = {
    val config = rootConfig.getConfig(path)

    val allKeyManagers = extractKeyManagers(config)
    val allTrustManagers = extractTrustManagers(config)

    SSLBuilder
      .empty()
      .withProtocol(config.as[String](Protocol))
      .withKeyManagers(allKeyManagers)
      .withTrustManagers(allTrustManagers)
  }

  private def extractKeyManagers(config: Config): Seq[X509KeyManager] = {
    // merge KeyStores with KeyStoresAndTrustStores
    val configs = (config.as[Seq[Config]](KeyStores.root) ++ config.as[Seq[Config]](KeyStoresAndTrustStores.root))
      .map { config =>
        val password = config.as[String](General.Password)

        val keyPassword = config.as[String](General.KeyPassword)

        val t = config.as[String](General.`Type`) match {
          case KeyStoreTypes.JKSType => JKSKeyStore(keyPassword)
          case KeyStoreTypes.PKCS12Type => PKCS12KeyStore(keyPassword)
        }

        StoreConfig(t, Paths.get(config.as[String](General.Path)), password)
      }

    configs
      .map { config =>
        KeyStoreUtils.openKeyStore(
          Files.newInputStream(config.path),
          config.password,
          config.keyStoreType
        )
      }
      .flatMap(_.keyManagers)

  }

  private def extractTrustManagers(config: Config): Seq[X509TrustManager] = {
    // merge TrustStores with KeyStoresAndTrustStores
    val configs = (config.as[Seq[Config]](TrustStores.root) ++ config.as[Seq[Config]](KeyStoresAndTrustStores.root))
      .map { config =>
        val t = config.as[String](General.`Type`) match {
          case KeyStoreTypes.JKSType => JKSTrustStore
          case KeyStoreTypes.PKCS12Type => PKCS12TrustStore
        }

        StoreConfig(t, Paths.get(config.as[String](General.Path)), config.as[String](General.Password))
      }

    configs
      .map { config =>
        KeyStoreUtils.openKeyStore(
          Files.newInputStream(config.path),
          config.password,
          config.keyStoreType
        )
      }
      .flatMap(_.trustManagers)
  }

  case class StoreConfig(keyStoreType: KeyStoreType, path: Path, password: String)

}
