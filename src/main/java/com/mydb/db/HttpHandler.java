package com.mydb.db;

import com.mydb.db.services.LSMService;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Slf4j
public class HttpHandler {

  private final LSMService lsmService;
  private final Vertx vertx;

  public HttpHandler(LSMService lsmService, Vertx vertx) {
    this.lsmService = lsmService;
    this.vertx = vertx;
  }

  public void handleUpdate(final RoutingContext context) {
    final var probeId = context.pathParam("probeId");
    final var payload = context.getBody();
    vertx.executeBlocking(
        fut -> fut.complete(lsmService.insert(probeId, payload)),
        false,
        res -> {
          if (res.succeeded()) {
            context.response().setStatusCode(OK.code()).end();
          } else {
            context.fail(res.cause());
          }
        }
    );
  }

  public void handleRead(final RoutingContext context) {
    final var probeId = context.pathParam("probeId");
    vertx.executeBlocking(
        fut -> fut.complete(lsmService.getData(probeId)),
        false,
        res -> {
          if (res.succeeded()) {
            final var data = Optional.ofNullable((String) res.result()).orElse("{}");
            context.response().putHeader("content-type", "application/json").end(data);
          } else {
            log.error(res.cause().toString());
            context.response().setStatusCode(NOT_FOUND.code()).end();
          }
        }
    );
  }

}
