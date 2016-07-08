package com.twitter.finagle.buoyant.http2

import com.twitter.finagle.Service
import com.twitter.finagle.netty4.{BufAsByteBuf, ByteBufAsBuf}
import com.twitter.finagle.transport.Transport
import com.twitter.logging.Logger
import com.twitter.io.Reader
import com.twitter.util.{Future, Promise, Return, Throw}
import io.netty.handler.codec.http2._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class ClientDispatcher(
  transport: Transport[Http2StreamFrame, Http2StreamFrame]
) extends Service[Request, Response] {

  private[this] val log = Logger.get(getClass.getName)

  // TODO handle overflow
  private[this] val _id = new AtomicInteger(1)
  private[this] def nextId() = _id.getAndAdd(2)

  case class Stream(manager: StreamState.Manager, response: Promise[Response])
  private[this] val streams = new ConcurrentHashMap[Int, Stream]

  private[this] def readLoop(): Future[Unit] =
    transport.read().flatMap { frame =>
      streams.get(frame.streamId) match {
        case null =>
          readLoop()

        case Stream(manager, response) =>
          val (s0, s1) = manager.recv(frame)

          (s0, s1, frame) match {

            /*
             * Response headers
             */

            case (StreamState.RemoteOpen(None),
              StreamState.RemoteClosed(),
              f: Http2HeadersFrame) =>
              val rsp = Response(ResponseHeaders(f.headers), None)
              response.setValue(rsp)
              readLoop()

            case (StreamState.RemoteOpen(None),
              StreamState.RemoteOpen(Some(StreamState.Recv(reader, trailers))),
              f: Http2HeadersFrame) =>
              val rsp = Response(ResponseHeaders(f.headers), Some(DataStream(reader, trailers)))
              response.setValue(rsp)
              readLoop()

            /*
             * Data
             */

            case (StreamState.RemoteOpen(Some(StreamState.Recv(rw, trailers))),
              StreamState.RemoteClosed(),
              f: Http2DataFrame) =>
              rw.write(ByteBufAsBuf.Owned(f.content)).flatMap { _ =>
                rw.close().flatMap { _ =>
                  trailers.setValue(None)
                  readLoop()
                }
              }

            case (StreamState.RemoteOpen(Some(StreamState.Recv(rw, _))),
              StreamState.RemoteOpen(Some(_)),
              f: Http2DataFrame) =>
              rw.write(ByteBufAsBuf.Owned(f.content)).flatMap { _ =>
                readLoop()
              }

            /*
             * Trailers
             */

            case (StreamState.RemoteOpen(Some(StreamState.Recv(rw, trailers))),
              StreamState.RemoteClosed(),
              f: Http2HeadersFrame) =>
              rw.close().flatMap { _ =>
                trailers.setValue(Some(Headers(f.headers)))
                readLoop()
              }

            case (s0, s1, f) => Future.exception(new IllegalStateException(s"$s0 -> $s1 ($f)"))
          }
      }
    }

  private[this] def writeLoop(streamId: Int, reader: Reader, trailers: Future[Option[Headers]]): Future[Unit] = {
    def loop(): Future[Unit] =
      reader.read(Int.MaxValue).flatMap {
        case None =>
          trailers.flatMap {
            case None =>
              val frame = new DefaultHttp2DataFrame(true /*eos*/ )
              frame.setStreamId(streamId)
              transport.write(frame)

            case Some(trailers) =>
              val frame = headers(trailers, true /*eos*/ )
              frame.setStreamId(streamId)
              transport.write(frame)
          }

        case Some(buf) =>
          val frame = new DefaultHttp2DataFrame(BufAsByteBuf.Owned(buf), false /*eos*/ )
          frame.setStreamId(streamId)
          transport.write(frame).before(loop())
      }

    loop()
  }

  private[this] val reading = readLoop()

  def apply(req: Request): Future[Response] = {
    val streamId = nextId()
    val state = new StreamState.Manager
    val rspp = Promise[Response]
    streams.putIfAbsent(streamId, Stream(state, rspp)) match {
      case null => // expected
      case state =>
        throw new IllegalStateException(s"stream $streamId already exists as $state")
    }

    val frame = headers(req.headers, req.data.isEmpty)
    frame.setStreamId(streamId)

    state.send(req) match {
      case (StreamState.Idle, StreamState.Open(_, Some(StreamState.Send(reader, trailers)))) =>
        transport.write(frame).flatMap { _ =>
          writeLoop(streamId, reader, trailers)
          rspp
        }

      case (StreamState.Idle, StreamState.RemoteOpen(_)) =>
        transport.write(frame).before(rspp)

      case (s0, s1) => Future.exception(new IllegalStateException(s"stream $streamId $s0 -> $s1"))
    }
  }

  private[this] def headers(orig: Headers, eos: Boolean): Http2HeadersFrame = {
    val headers = orig match {
      case h: Netty4Headers => h.underlying
      case hs =>
        val headers = new DefaultHttp2Headers
        for ((k, v) <- hs.toSeq) headers.add(k, v)
        headers
    }
    new DefaultHttp2HeadersFrame(headers, eos)
  }
}
