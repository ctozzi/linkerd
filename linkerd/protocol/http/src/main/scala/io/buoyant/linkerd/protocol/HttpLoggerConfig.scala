package io.buoyant.linkerd.protocol

import com.fasterxml.jackson.annotation.JsonIgnore
import com.twitter.finagle.{Filter, Service, ServiceFactory, Stack, Stackable}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.Future
import io.buoyant.config.PolymorphicConfig

abstract class HttpLoggerConfig extends PolymorphicConfig {
  @JsonIgnore
  def mk(params: Stack.Params): Filter[Request, Response, Request, Response]
}

object HttpLoggerConfig {
  object param {
    case class Logger(logger: (Stack.Params) => Filter[Request, Response, Request, Response])
    implicit object Logger extends Stack.Param[Logger] {
      val default = Logger((stack: Stack.Params) => Filter.identity)
    }
  }

  def module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module[ServiceFactory[Request, Response]] {
      override val role = Stack.Role("HttpLogger")
      override val description = "HTTP Logger"
      override val parameters = Seq(implicitly[Stack.Param[HttpLoggerConfig.param.Logger]])
      def make(params: Stack.Params, stack: Stack[ServiceFactory[Request, Response]]): Stack[ServiceFactory[Request, Response]] = {
        val loggerP = params[HttpLoggerConfig.param.Logger]
        val filter = loggerP.logger(params)
        val svcFactory = filter.andThen(stack.make(params))
        Stack.Leaf(role, svcFactory)
      }
    }
}
