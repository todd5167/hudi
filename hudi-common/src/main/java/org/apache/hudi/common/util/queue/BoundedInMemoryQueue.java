/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util.queue;

import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 *   队列通过其占用的内存字节来限制大小。 标准实现受限于队列中的条目数。
 * Used for enqueueing（排队） input records. Queue limit is controlled by {@link #memoryLimit}. Unlike standard bounded queue
 * implementations, this queue bounds the size by memory bytes occupied by its tenants. The standard implementation
 * bounds by the number of entries in the queue.
 *
 *    它在内部对每第 {@link #RECORD_SAMPLING_RATE} 个记录进行采样，并相应地调整队列中的记录数。 这样做是为了确保我们不会 OOM。
 * It internally samples every {@link #RECORD_SAMPLING_RATE}th record and adjusts number of records in queue
 * accordingly. This is done to ensure that we don't OOM.
 *
 *    该队列支持多生产者单消费者模式。
 * This queue supports multiple producer single consumer pattern.
 *
 * @param <I> input payload data type
 * @param <O> output payload data type
 */
public class BoundedInMemoryQueue<I, O> implements Iterable<O> {

  /**
   *  用于轮询队列中记录的间隔。
   * Interval used for polling records in the queue. **/
  public static final int RECORD_POLL_INTERVAL_SEC = 1;

  /**
   * 用于采样记录以确定平均记录大小（以字节为单位）的速率。
   * Rate used for sampling records to determine avg record size in bytes. **/
  public static final int RECORD_SAMPLING_RATE = 64;

  /** Maximum records that will be cached. **/
  private static final int RECORD_CACHING_LIMIT = 128 * 1024;

  private static final Logger LOG = LogManager.getLogger(BoundedInMemoryQueue.class);

  /**
   *  它指示要缓存的记录数。 我们将使用采样记录的平均大小来确定我们应该缓存多少记录，并将相应地更改（增加/减少）许可。
   * It indicates number of records to cache. We will be using sampled record's average size to
   * determine how many records we should cache and will change (increase/decrease) permits accordingly.
   */
  public final Semaphore rateLimiter = new Semaphore(1);

  /** Used for sampling records with "RECORD_SAMPLING_RATE" frequency. **/
  public final AtomicLong samplingRecordCounter = new AtomicLong(-1);

  /** Internal queue for records. **/
  private final LinkedBlockingQueue<Option<O>> queue = new LinkedBlockingQueue<>();

  /** Maximum amount of memory to be used for queueing records. **/
  private final long memoryLimit;

  /**
   * it holds the root cause of the Throwable in case either queueing records
   * (consuming from inputIterator) fails or thread reading records from queue fails.
   */
  private final AtomicReference<Throwable> hasFailed = new AtomicReference<>(null);

  /**
   *  用于指示队列中的所有记录都读取成功。
   * Used for indicating that all the records from queue are read successfully. **/
  private final AtomicBoolean isReadDone = new AtomicBoolean(false);

  /**
   *  用于表示所有记录都已入队。
   * used for indicating that all records have been enqueued. **/
  private final AtomicBoolean isWriteDone = new AtomicBoolean(false);

  /**
   *  将数据数据转换为输出数据的函数
   * Function to transform the input payload to the expected output payload. **/
  private final Function<I, O> transformFunction;

  /**
   *   负载估算器
   * Payload Size Estimator. **/
  private final SizeEstimator<O> payloadSizeEstimator;

  /**
   * 内存队列的迭代器
   * Singleton (w.r.t this instance) Iterator for this queue. **/
  private final QueueIterator iterator;

  /**
   * indicates rate limit (number of records to cache). it is updated
   * whenever there is a change in avg record size.
   */
  public int currentRateLimit = 1;

  /**
   * 表示了每条记录所占字节大小，在记录采样后被更新
   * Indicates avg record size in bytes. It is updated whenever a new record is sampled. **/
  public long avgRecordSizeInBytes = 0;

  /** Indicates number of samples collected so far. **/
  private long numSamples = 0;

  /**
   * Construct BoundedInMemoryQueue with default SizeEstimator.
   *
   * @param memoryLimit MemoryLimit in bytes
   * @param transformFunction Transformer Function to convert input payload type to stored payload type
   */
  public BoundedInMemoryQueue(final long memoryLimit, final Function<I, O> transformFunction) {
    this(memoryLimit, transformFunction, new DefaultSizeEstimator() {});
  }

  /**
   * Construct BoundedInMemoryQueue with passed in size estimator.
   *
   * @param memoryLimit MemoryLimit in bytes
   * @param transformFunction Transformer Function to convert input payload type to stored payload type
   * @param payloadSizeEstimator Payload Size Estimator
   */
  public BoundedInMemoryQueue(final long memoryLimit, final Function<I, O> transformFunction,
      final SizeEstimator<O> payloadSizeEstimator) {
    this.memoryLimit = memoryLimit;
    this.transformFunction = transformFunction;
    this.payloadSizeEstimator = payloadSizeEstimator;
    this.iterator = new QueueIterator();
  }

  public int size() {
    return this.queue.size();
  }

  /**
   *
   *   以“RECORD_SAMPLING_RATE”频率对记录进行采样并计算平均记录大小（以字节为单位）。
   *   它用于确定队列的最大记录数。 根据平均规模的变化，它可能会增加或减少可用许可。
   *
   * Samples records with "RECORD_SAMPLING_RATE" frequency and computes average record size in bytes. It is used for
   * determining how many maximum records to queue. Based on change in avg size it ma increase or decrease available
   * permits.
   *
   * @param payload Payload to size
   */
  private void adjustBufferSizeIfNeeded(final O payload) throws InterruptedException {
    //   64条记录采样一条
    if (this.samplingRecordCounter.incrementAndGet() % RECORD_SAMPLING_RATE != 0) {
      return;
    }
    final long recordSizeInBytes = payloadSizeEstimator.sizeEstimate(payload);
    // 基于历史数据计算记录的平均字节大小
    final long newAvgRecordSizeInBytes =
        Math.max(1, (avgRecordSizeInBytes * numSamples + recordSizeInBytes) / (numSamples + 1));

    // 当前允许缓存的数据条数
    final int newRateLimit =
        (int) Math.min(RECORD_CACHING_LIMIT, Math.max(1, this.memoryLimit / newAvgRecordSizeInBytes));

    //  如果要缓存的记录数量有任何变化，那么我们将释放（如果增加）或获取（如果减少）以将速率限制调整为新计算的值。
    // If there is any change in number of records to cache then we will either release (if it increased) or acquire
    // (if it decreased) to adjust rate limiting to newly computed value.
    if (newRateLimit > currentRateLimit) {
      //  增加信号量
      rateLimiter.release(newRateLimit - currentRateLimit);
    } else if (newRateLimit < currentRateLimit) {
      //  减少信号量
      rateLimiter.acquire(currentRateLimit - newRateLimit);
    }

    currentRateLimit = newRateLimit;

    avgRecordSizeInBytes = newAvgRecordSizeInBytes;

    numSamples++;
  }

  /**
   *
   *  将 record 应用 transformation后存储到队列
   * Inserts record into queue after applying transformation.
   *
   * @param t Item to be queued
   */
  public void insertRecord(I t) throws Exception {
    // If already closed, throw exception
    if (isWriteDone.get()) {
      throw new IllegalStateException("Queue closed for enqueueing new entries");
    }

    // We need to stop queueing if queue-reader has failed and exited.
    throwExceptionIfFailed();
    //  获取信号量 -1
    rateLimiter.acquire();
    // We are retrieving insert value in the record queueing thread to offload computation
    // around schema validation
    // and record creation to it.

    //  HoodieRecord ---->  HoodieInsertValueGenResult
    final O payload = transformFunction.apply(t);

    adjustBufferSizeIfNeeded(payload);

    queue.put(Option.of(payload));
  }

  /**
   *  检查记录是否在队列中可用或预计将来会写入。
   * Checks if records are either available in the queue or expected to be written in future.
   */
  private boolean expectMoreRecords() {
    return !isWriteDone.get() || (isWriteDone.get() && !queue.isEmpty());
  }

  /**
   *   读取器，但从未暴露于外部，因为这是一个单一的消费者队列。 读取是通过此队列的单例迭代器完成的。
   * Reader interface but never exposed to outside world as this is a single consumer queue. Reading is done through a
   * singleton iterator for this queue.
   */
  private Option<O> readNextRecord() {
    if (this.isReadDone.get()) {
      return Option.empty();
    }
    // 释放信号量
    rateLimiter.release();

    Option<O> newRecord = Option.empty();
    while (expectMoreRecords()) {
      try {
        throwExceptionIfFailed();
        // 带有超时时间的poll
        newRecord = queue.poll(RECORD_POLL_INTERVAL_SEC, TimeUnit.SECONDS);
        if (newRecord != null) {
          break;
        }
      } catch (InterruptedException e) {
        LOG.error("error reading records from queue", e);
        throw new HoodieException(e);
      }
    }
    // Check one more time here as it is possible producer erred out and closed immediately
    throwExceptionIfFailed();

    if (newRecord != null && newRecord.isPresent()) {
      return newRecord;
    } else {
      // We are done reading all the records from internal iterator.
      this.isReadDone.set(true);
      return Option.empty();
    }
  }

  /**
   *
   *  Write done in batch
   * Puts an empty entry to queue to denote termination.
   */
  public void close() {
    // done queueing records notifying queue-reader.
    isWriteDone.set(true);
  }

  private void throwExceptionIfFailed() {
    if (this.hasFailed.get() != null) {
      close();
      throw new HoodieException("operation has failed", this.hasFailed.get());
    }
  }

  /**
   * API to allow producers and consumer to communicate termination due to failure.
   */
  public void markAsFailed(Throwable e) {
    this.hasFailed.set(e);
    // release the permits so that if the queueing thread is waiting for permits then it will
    // get it.
    this.rateLimiter.release(RECORD_CACHING_LIMIT + 1);
  }

  @Override
  public Iterator<O> iterator() {
    return iterator;
  }

  /**
   * Iterator for the memory bounded queue.
   */
  private final class QueueIterator implements Iterator<O> {

    // next record to be read from queue.
    private O nextRecord;

    @Override
    public boolean hasNext() {
      if (this.nextRecord == null) {
        Option<O> res = readNextRecord();
        this.nextRecord = res.orElse(null);
      }
      return this.nextRecord != null;
    }

    @Override
    public O next() {
      ValidationUtils.checkState(hasNext() && this.nextRecord != null);
      final O ret = this.nextRecord;
      this.nextRecord = null;
      return ret;
    }
  }
}
