package com.mydb.app;

import com.mydb.app.verticle.LSMVerticle;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {

  public static void main(final String[] args) {
    Vertx vertex = Vertx.vertx();

    var deploymentFuture = vertex.deployVerticle(new LSMVerticle());
    deploymentFuture.onFailure(it -> {
      log.error("Unable to start the application", deploymentFuture.cause());
      Runtime.getRuntime().exit(-1);
    });

  }
}
