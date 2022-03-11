package com.mydb.app.verticle;

import com.mydb.db.HttpHandler;
import com.mydb.db.SchedulerConfig;
import com.mydb.db.StateLoader;
import com.mydb.db.entity.MemTableWrapper;
import com.mydb.db.entity.merge.SegmentGenerator;
import com.mydb.db.services.FileIOService;
import com.mydb.db.services.LSMService;
import com.mydb.db.services.MergeService;
import com.mydb.db.services.SegmentService;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;
import org.quartz.SchedulerException;

import java.util.Optional;

@Slf4j
public class LSMVerticle extends AbstractVerticle {

  @Override
  public void start(final Promise<Void> promise) {
    ConfigRetriever.create(vertx).getConfig()
        .compose(this::boot)
        .onSuccess(s -> promise.complete())
        .onFailure(e -> promise.fail(e.getMessage()));
  }

  private Future<io.vertx.core.http.HttpServer> boot(final JsonObject config) {
    final var fileIOService = new FileIOService();
    final var stateLoader = new StateLoader(fileIOService);
    final var segmentConfig = stateLoader.getSegmentConfig();
    final var indices = stateLoader.getIndices();
    final var memTableData = stateLoader.getMemTableDataFromWAL();
    final var mergeService = new MergeService(fileIOService);
    final var segmentService = new SegmentService(segmentConfig, fileIOService);
    final var segmentGenerator = new SegmentGenerator(fileIOService, segmentService, 50000, 400000);
    final var memTableWrapper = new MemTableWrapper(
        memTableData, indices, fileIOService, segmentGenerator);
    final var lsmService = new LSMService(memTableWrapper, indices, fileIOService, segmentService, mergeService);
    final var httpHandler = new HttpHandler(lsmService, vertx);
    setupScheduledMerging(config, lsmService);
    Integer port = Optional.ofNullable(config.getJsonObject("http"))
        .map(it -> it.getInteger("port"))
        .orElse(8080);

    return vertx.createHttpServer()
        .requestHandler(defineRoutes(vertx, httpHandler))
        .listen(port)
        .onSuccess(server -> {
          log.info("Started mydb http server on port - {}", port);
        })
        .onFailure(e -> {
          log.error("Fatal error! Failed to start server!");
        });
  }

  private void setupScheduledMerging(final JsonObject config, final LSMService lsmService) {
    try {
      new SchedulerConfig().scheduleMergeSegments(config, lsmService);
    } catch (SchedulerException schedulerException) {
      log.error("error occurred while setting up scheduling", schedulerException);
    }
  }

  public Router defineRoutes(final Vertx vertx, final HttpHandler handler) {
    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());

    addRoutes(router, handler);

    return router;
  }

  private void addRoutes(final Router router, final HttpHandler handler) {
    router.route(HttpMethod.PUT, "/probe/:probeId/event/:eventId")
        .handler(handler::handleUpdate);

    router.route(HttpMethod.GET, "/probe/:probeId/latest")
        .handler(handler::handleRead);
  }
}
