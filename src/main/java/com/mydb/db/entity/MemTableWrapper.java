package com.mydb.db.entity;

import com.mydb.db.entity.merge.SegmentGenerator;
import com.mydb.db.services.FileIOService;
import io.vertx.core.buffer.Buffer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;

@Getter
@Setter
@Slf4j
public class MemTableWrapper {

  private Deque<SegmentIndex> indices;
  private SegmentGenerator generator;
  private FileIOService fileIOService;
  private Deque<String> probeIds;
  private Map<String, Deque<Buffer>> memTable;

  public MemTableWrapper(
      ImmutablePair<Deque<String>, Map<String, Deque<Buffer>>> memTableData,
      Deque<SegmentIndex> indices,
      FileIOService fileIOService,
      SegmentGenerator generator
  ) {
    this.indices = indices;
    this.fileIOService = fileIOService;
    this.generator = generator;
    this.probeIds = memTableData.getLeft();
    this.memTable = memTableData.getRight();
  }

  public CompletableFuture<Boolean> persist(final String probeId, final Buffer payload) {
    return fileIOService.writeAheadLog(payload)
        .thenApply(b -> put(probeId, payload))
        .thenApply(b -> generator.update(indices, probeIds, memTable));
  }

  private boolean put(final String probeId, final Buffer payload) {
    probeIds.addLast(probeId);
    if (memTable.containsKey(probeId)) {
      memTable.get(probeId).addLast(payload);
    } else {
      var list = new ConcurrentLinkedDeque<Buffer>();
      list.addLast(payload);
      memTable.put(probeId, list);
    }
    return true;
  }

  public Buffer get(final String probeId) {
    var list = memTable.getOrDefault(probeId, null);
    if (list != null && !list.isEmpty()) {
      return list.getLast();
    }
    return null;
  }
}
