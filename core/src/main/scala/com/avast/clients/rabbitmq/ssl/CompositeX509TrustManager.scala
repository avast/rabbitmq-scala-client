package com.avast.clients.rabbitmq.ssl

import java.security.cert.{CertificateException, X509Certificate}
import javax.net.ssl.X509TrustManager

import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._

/** Represents an ordered list of `X509TrustManager`s with additive trust. If any one of the
  * composed managers trusts a certificate chain, then it is trusted by the composite manager.
  * This is necessary because of the fine-print on [[javax.net.ssl.SSLContext]]:
  * Only the first instance of a particular key and/or trust manager implementation type in the
  * array is used. (For example, only the first javax.net.ssl.X509KeyManager in the array will be used.)
  *
  * @author codyaray
  * @see <a href="http://stackoverflow.com/questions/1793979/registering-multiple-keystores-in-jvm">StackOverflow</a>
  * @since 4/22/2013
  */
private[ssl] class CompositeX509TrustManager(trustManagers: Seq[X509TrustManager]) extends X509TrustManager with StrictLogging {
  def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {
    require(chain.nonEmpty, "Required non-empty chain")

    logger.trace(s"Verifying given SSL certificate: ${chain.mkString("[", ", ", "]")}")

    val ok = trustManagers.exists { trustManager =>
      try {
        trustManager.checkClientTrusted(chain, authType)
        true
      } catch {
        case e: CertificateException => // maybe someone else will trust them
          logger.trace(s"$trustManager doesn't trust the chain", e)
          false
      }
    }

    if (!ok) throw new CertificateException("None of the TrustManagers trust this certificate chain")
  }

  def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {
    require(chain.nonEmpty, "Required non-empty chain")

    logger.debug(s"Verifying certificate chain ${formatChain(chain)}")
    logger.trace(s"Verifying given SSL certificate: ${chain.mkString("[", ", ", "]")}")

    val ok = trustManagers.exists { trustManager =>
      try {
        trustManager.checkServerTrusted(chain, authType)
        logger.debug(s"$trustManager verified the SSL connection")
        true
      } catch {
        case e: CertificateException => // maybe someone else will trust them
          logger.trace(s"$trustManager doesn't trust the chain", e)
          false
      }
    }

    if (!ok) throw new CertificateException("None of the TrustManagers trust this certificate chain")
  }

  def getAcceptedIssuers: Array[X509Certificate] = trustManagers.flatMap(_.getAcceptedIssuers).toArray

  private def formatChain(chain: Array[X509Certificate]): String =
    chain
      .map { cert =>
        val sdn = cert.getSubjectDN.getName
        val san = Option(cert.getSubjectAlternativeNames)
          .map(_.asScala)
          .map { names =>
            names.mkString("(", ", ", ")")
          }
          .getOrElse("()")
        val issuer = cert.getIssuerDN.getName

        s"{$sdn, Issuer=[$issuer], SAN=$san}"
      }
      .mkString("[", ", ", "]")
}
