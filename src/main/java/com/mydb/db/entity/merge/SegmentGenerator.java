package com.mydb.db.entity.merge;

import com.mydb.db.entity.SegmentIndex;
import com.mydb.db.services.FileIOService;
import com.mydb.db.services.LSMService;
import com.mydb.db.services.SegmentService;
import io.vertx.core.buffer.Buffer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.util.SerializationUtils;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import static java.util.concurrent.CompletableFuture.supplyAsync;

@Slf4j
public class SegmentGenerator {

  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private final Lock lock = new ReentrantLock();
  private final int memTableSoftLimit;
  private final int memTableHardLimit;

  public SegmentGenerator(
      FileIOService fileIOService, SegmentService segmentService,
      int memTableSoftLimit,
      int memTableHardLimit
  ) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
    this.memTableSoftLimit = memTableSoftLimit;
    this.memTableHardLimit = memTableHardLimit;
  }

  public boolean update(
      Deque<SegmentIndex> indices,
      Deque<String> probeIds,
      Map<String, Deque<Buffer>> memTable
  ) {
    return generate(indices, probeIds, memTable);
  }

  private boolean generate(
      Deque<SegmentIndex> indices,
      Deque<String> probeIds,
      Map<String, Deque<Buffer>> memTable
  ) {
    if (lock.tryLock()) {
      try {
        final var size = probeIds.size();
        if (isMemTableFull(size)) {
          updateHardLimitBreach(size);
          flushMultipleSegments(indices, probeIds, memTable, size);
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      } finally {
        lock.unlock();
      }
    }
    return true;
  }


  private void flushMultipleSegments(
      final Deque<SegmentIndex> indices,
      final Deque<String> probeIds,
      final Map<String, Deque<Buffer>> memTable,
      final int size
  ) {
    LinkedList<ImmutablePair<Integer, Integer>> ranges = getRanges(size);

    var segmentsWritten = ranges.parallelStream()
        .map(range -> ImmutablePair.of(range, segmentService.getNewSegment()))
        .map(pair -> supplyAsync(
            () -> ImmutablePair.of(fileIOService.persist(pair.right, probeIds, memTable, pair.left), pair.right))
        )
        .map(CompletableFuture::join)
        .map(pair -> {
          updateIndices(indices, pair.left);
          return pair.right;
        }).toList();

    clearProbeIds(probeIds, ImmutablePair.of(0, ranges.getLast().right));
    updateHardLimitBreach(probeIds.size());

    final var lastSegment = segmentsWritten.get(segmentsWritten.size() - 1);
    supplyAsync(() -> fileIOService.persistIndices(
        lastSegment.getBackupPath(), SerializationUtils.serialize(indices)
    ));

    FileIOService.STAGED_WAL_FILE.delete();
    FileIOService.WAL_FILE.renameTo(FileIOService.STAGED_WAL_FILE);
  }

  private void clearProbeIds(
      Deque<String> probeIds,
      ImmutablePair<Integer, Integer> range
  ) {
    IntStream.range(range.left, range.right).parallel().forEach(i -> {
        try {
          probeIds.removeFirst();
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    );
  }

  private void updateIndices(Deque<SegmentIndex> indices, SegmentIndex s) {
    indices.addFirst(s);
  }

  private boolean isMemTableFull(final int payloadCount) {
    return payloadCount >= memTableSoftLimit;
  }

  public void updateHardLimitBreach(final int payloadCount) {
    LSMService.hardLimitBreached = payloadCount >= memTableHardLimit;
  }

  private LinkedList<ImmutablePair<Integer, Integer>> getRanges(int size) {
    var ranges = new LinkedList<ImmutablePair<Integer, Integer>>();
    var start = 0;
    var end = memTableSoftLimit;
    while (size >= end) {
      ranges.add(ImmutablePair.of(start, end));
      start = end + 1;
      end += memTableSoftLimit;
    }
    return ranges;
  }

}
