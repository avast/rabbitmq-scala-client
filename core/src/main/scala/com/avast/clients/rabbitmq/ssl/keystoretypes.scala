package com.avast.clients.rabbitmq.ssl

sealed trait KeyStoreType {
  def keyStoreType: String
}

/**
  * Types of keystore bundles.
  */
object KeyStoreTypes {

  final val JKSType = "JKS"
  final val PKCS12Type = "PKCS12"

  sealed trait KeyStore extends KeyStoreType {
    def keysPassword: String
  }

  sealed trait TrustStore extends KeyStoreType

  case class JKSKeyStore(keysPassword: String) extends KeyStore {
    override def keyStoreType: String = JKSType
  }

  case class JKSKeyAndTrustStore(keysPassword: String) extends KeyStore with TrustStore {
    override def keyStoreType: String = JKSType
  }

  case object JKSTrustStore extends TrustStore {
    override def keyStoreType: String = JKSType
  }

  case class PKCS12KeyStore(keysPassword: String) extends KeyStore {
    override def keyStoreType: String = PKCS12Type
  }

  case class PKCS12KeyAndTrustStore(keysPassword: String) extends KeyStore with TrustStore {
    override def keyStoreType: String = PKCS12Type
  }

  case object PKCS12TrustStore extends TrustStore {
    override def keyStoreType: String = PKCS12Type
  }

}
