package io.buoyant.linkerd.protocol.http

import com.twitter.finagle.buoyant.H2
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.util.LoadService
import com.twitter.finagle.{Service, Stack}
import com.twitter.util.{Duration, Future}
import io.buoyant.config.Parser
import io.buoyant.grpc.runtime.Stream
import io.buoyant.k8s.istio.MixerClient
import io.buoyant.linkerd.LoggerInitializer
import io.buoyant.linkerd.protocol.HttpLoggerConfig
import io.buoyant.test.Awaits
import istio.mixer.v1.ReportResponse
import org.scalatest.FunSuite

class MockMixerClient extends MixerClient(H2.client.newService("dest")) {
  var reports = 0

  override def report(
    responseCode: Int,
    requestPath: String,
    targetService: String,
    sourceLabelApp: String,
    targetLabelApp: String,
    targetLabelVersion: String,
    duration: Duration
  ): Stream[ReportResponse] = {
    reports += 1
    Stream.value(ReportResponse())
  }
}

class IstioLoggerTest extends FunSuite with Awaits {
  val mixerClient = new MockMixerClient()

  test("creates a logger") {
    val logger = new IstioLogger(mixerClient, Stack.Params.empty)
    assert(mixerClient.reports == 0)
  }

  test("apply triggers a mixer report") {
    val logger = new IstioLogger(mixerClient, Stack.Params.empty)
    val svc = Service.mk[Request, Response] { req =>
      Future.value(Response())
    }

    assert(mixerClient.reports == 0)
    logger(Request(), svc)
    assert(mixerClient.reports == 1)
  }
}

class IstioLoggerConfigTest extends FunSuite with Awaits {
  test("sanity") {
    // ensure it doesn't totally blow up
    val _ = new IstioLoggerConfig(None, None).mk(Stack.Params.empty)
  }

  test("service registration") {
    assert(LoadService[LoggerInitializer].exists(_.isInstanceOf[IstioLoggerInitializer]))
  }

  test("parse config") {
    val yaml =
      s"""|kind: io.l5d.istio
      """.stripMargin

    val mapper = Parser.objectMapper(yaml, Iterable(Seq(IstioLoggerInitializer)))
    val config = mapper.readValue[HttpLoggerConfig](yaml).asInstanceOf[IstioLoggerConfig]
    val logger = config.mk(Stack.Params.empty)
    // val req = Request()
    // req.headerMap.set("my-header", "/one/two/three")
    // assert(
    //   await(identifier(req)).asInstanceOf[IdentifiedRequest[Request]].dst ==
    //     Dst.Path(Path.read("/one/two/three"))
    // )
  }

  test("verify default") {
    val yaml =
      s"""|kind: io.l5d.istio
      """.stripMargin

    val mapper = Parser.objectMapper(yaml, Iterable(Seq(IstioLoggerInitializer)))
    val config = mapper.readValue[HttpLoggerConfig](yaml).asInstanceOf[IstioLoggerConfig]
    val logger = config.mk(Stack.Params.empty)
    assert(config.host == config.DefaultMixerHost)
    assert(config.port == config.DefaultMixerPort)
    // val req = Request()
    // req.headerMap.set("l5d-name", "/one/two/three")
    // assert(
    //   await(identifier(req)).asInstanceOf[IdentifiedRequest[Request]].dst ==
    //     Dst.Path(Path.read("/one/two/three"))
    // )
  }

  test("overrides defaults") {
    val yaml =
      s"""|kind: io.l5d.istio
          |mixerHost: mixerHost
          |mixerPort: 1234
      """.stripMargin

    val mapper = Parser.objectMapper(yaml, Iterable(Seq(IstioLoggerInitializer)))
    val config = mapper.readValue[HttpLoggerConfig](yaml).asInstanceOf[IstioLoggerConfig]
    val logger = config.mk(Stack.Params.empty)
    assert(config.host == "mixerHost")
    assert(config.port == 1234)
    // val req = Request()
    // req.headerMap.set("l5d-name", "/one/two/three")
    // assert(
    //   await(identifier(req)).asInstanceOf[IdentifiedRequest[Request]].dst ==
    //     Dst.Path(Path.read("/one/two/three"))
    // )
  }
}
