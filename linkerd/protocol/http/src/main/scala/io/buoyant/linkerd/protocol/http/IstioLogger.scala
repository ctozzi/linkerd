package io.buoyant.linkerd.protocol.http

import com.fasterxml.jackson.annotation.JsonIgnore
import com.twitter.finagle.buoyant.H2
import com.twitter.finagle.client.Transporter.EndpointAddr
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle._
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util._
import io.buoyant.config.types.Port
import io.buoyant.k8s.istio.MixerClient
import io.buoyant.linkerd.LoggerInitializer
import io.buoyant.linkerd.protocol.HttpLoggerConfig
import io.buoyant.router.context.DstBoundCtx

object IstioLogger {
  val unknown = "unknown"

  // expected DstBoundCtx.current:
  // Some((Path(%,io.l5d.k8s.daemonset,default,incoming,l5d,#,io.l5d.k8s.istio,reviews.default.svc.cluster.local,az:us-west::env:prod::version:v1,http),Path()))
  val pathLength = 10
  val pathServiceIndex = 7
  val pathLabelsIndex = 8
}

class IstioLogger(mixerClient: MixerClient, params: Stack.Params) extends Filter[Request, Response, Request, Response] {
  import IstioLogger._

  private[this] val log = Logger.get()

  def apply(req: Request, svc: Service[Request, Response]) = {
    val start = Time.now

    svc(req).respond { ret =>
      val duration = Time.now - start
      val responseCode = {
        ret match {
          case Return(rsp) => rsp
          case Throw(e) =>
            // map exceptions to 500
            log.warning(s"IstioLogger exception: $e")
            Response(com.twitter.finagle.http.Status.InternalServerError)
        }
      }.statusCode

      // check for valid istio path
      val istioPath = DstBoundCtx.current match {
        case Some(bound) =>
          bound.id match {
            case path: Path if (path.elems.length == pathLength) => Some(path)
            case _ => None
          }
        case None => None
      }

      val targetService = istioPath match {
        case Some(path) =>
          val elem = path.showElems(pathServiceIndex)
          if (elem.indexOf(".svc.cluster.local") != -1) {
            elem
          } else {
            unknown
          }
        case None => unknown
      }

      val version = istioPath match {
        case Some(path) =>
          path.showElems(pathLabelsIndex).split("::").find {
            e => e.indexOf("version:") == 0
          } match {
            case Some(label: String) => label.split(":").last
            case _ => unknown
          }
        case None => unknown
      }

      val targetLabelApp = if (targetService != unknown) {
        targetService.split('.').head
      } else {
        unknown
      }

      val _ = mixerClient.report(
        responseCode,
        req.path,
        targetService, // target in prom (~reviews.default.svc.cluster.local)
        unknown, // source in prom (~productpage)
        targetLabelApp, // service in prom (~reviews)
        version, // version in prom (~v1)
        duration
      )
    }
  }
}

case class IstioLoggerConfig(
  mixerHost: Option[String],
  mixerPort: Option[Port]
) extends HttpLoggerConfig {
  @JsonIgnore
  val DefaultMixerHost = "istio-mixer.default.svc.cluster.local"
  @JsonIgnore
  val DefaultMixerPort = 9091

  @JsonIgnore
  private[this] val log = Logger.get("IstioLoggerConfig")

  val host = mixerHost.getOrElse(DefaultMixerHost)
  val port = mixerPort.map(_.port).getOrElse(DefaultMixerPort)
  log.info(s"connecting to Istio Mixer at $host:$port")
  val mixerDst = Name.bound(Address(host, port))
  val mixerService = H2.client
    .withParams(H2.client.params)
    .newService(mixerDst, "istioLogger")
  val client = new MixerClient(mixerService)

  def mk(params: Stack.Params): Filter[Request, Response, Request, Response] = {
    new IstioLogger(client, params)
  }
}

class IstioLoggerInitializer extends LoggerInitializer {
  val configClass = classOf[IstioLoggerConfig]
  override val configId = "io.l5d.istio"
}

object IstioLoggerInitializer extends IstioLoggerInitializer
