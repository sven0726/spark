package org.apache.spark.deploy

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging

/**
 * Created by Qiniu on 31/7/2017.
 */
object XSparkUI extends Logging{

  private final val ENV_XSPARK_AGENT = "XSPARK_AGENT"

  final val WORKER_DOMAIN = "worker_domain"
  final val DRIVER_DOMAIN = "driver_domain"
  final val CLUSTER_DOMAIN = "spark_cluster_domain"
  final val POUND = "#"

  def retrieveXSparkAP(domainType: String): String = synchronized {
    sys.env.foreach { env =>
      logDebug(env._1 + ":"  + env._2)
    }
    try {
      val xsparkAgentHost = sys.env.get(ENV_XSPARK_AGENT).getOrElse(POUND)
      val domain_api = s"http://${xsparkAgentHost}/api/domain_mapping"
      logDebug(s"Query domain mapping from ${domain_api} ...")
      val result = scala.io.Source.fromURL(domain_api).mkString
      val domain_mapping = scala.util.parsing.json.JSON.parseFull(result)
      val apDomain = domain_mapping match {
        case Some(m: Map[String, Any]) => m(domainType) match {
          case domain: String =>
            logInfo(s"Get domain ${domainType} is ${domain}")
            if(domainType == WORKER_DOMAIN) {
              return domain.replace("http://", "")
            }
            return domain
          case None => POUND
        }
      }
      apDomain.toString()
    }
    catch {
      case e: Exception =>
        logError("Failed to retrieve xspark ap domain", e)
        // We would rather return POUND（#） than throw exceptions to ui
        POUND
    }
  }

}