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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.SchemeAwareFSDataInputStream;
import org.apache.hudi.common.fs.TimedFSDataInputStream;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieCorruptBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieHFileDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.CorruptedLogFileException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *   1. 读取log文件，返回Block
 *
 *
 *  扫描日志文件并在日志文件上提供块级迭代器 将整个块内容加载到内存中 可以发出 DataBlock、CommandBlock、DeleteBlock 或 CorruptBlock（如果找到）。
 *
 * Scans a log file and provides block level iterator on the log file Loads the entire block contents in memory Can emit
 * either a DataBlock, CommandBlock, DeleteBlock or CorruptBlock (if one is found).
 */
public class HoodieLogFileReader implements HoodieLogFormat.Reader {

  public static final int DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024; // 16 MB
  private static final int BLOCK_SCAN_READ_BUFFER_SIZE = 1024 * 1024; // 1 MB
  private static final Logger LOG = LogManager.getLogger(HoodieLogFileReader.class);

  private final FSDataInputStream inputStream;
  private final HoodieLogFile logFile;
  private final byte[] magicBuffer = new byte[6];
  private final Schema readerSchema;
  private final String keyField;
  private boolean readBlockLazily;
  private long reverseLogFilePosition;
  private long lastReverseLogFilePosition;
  private boolean reverseReader;
  private boolean enableInlineReading;
  private boolean closed = false;
  private transient Thread shutdownThread = null;

  public HoodieLogFileReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema, int bufferSize,
                             boolean readBlockLazily) throws IOException {
    this(fs, logFile, readerSchema, bufferSize, readBlockLazily, false);
  }

  public HoodieLogFileReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema, int bufferSize,
                             boolean readBlockLazily, boolean reverseReader) throws IOException {
    this(fs, logFile, readerSchema, bufferSize, readBlockLazily, reverseReader, false,
        HoodieRecord.RECORD_KEY_METADATA_FIELD);
  }

  public HoodieLogFileReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema, int bufferSize,
                             boolean readBlockLazily, boolean reverseReader, boolean enableInlineReading,
                             String keyField) throws IOException {
    // 最大读 默认16M
    FSDataInputStream fsDataInputStream = fs.open(logFile.getPath(), bufferSize);
    this.logFile = logFile;
    this.inputStream = getFSDataInputStream(fsDataInputStream, fs, bufferSize);
    this.readerSchema = readerSchema;
    this.readBlockLazily = readBlockLazily;
    this.reverseReader = reverseReader;
    this.enableInlineReading = enableInlineReading;
    this.keyField = keyField;
    if (this.reverseReader) {
      this.reverseLogFilePosition = this.lastReverseLogFilePosition = logFile.getFileSize();
    }
    addShutDownHook();
  }

  public HoodieLogFileReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema) throws IOException {
    this(fs, logFile, readerSchema, DEFAULT_BUFFER_SIZE, false, false);
  }

  /**
   *    通过包装所需的输入流来获取要使用的正确 {@link FSDataInputStream}。
   * Fetch the right {@link FSDataInputStream} to be used by wrapping with required input streams.
   * @param fsDataInputStream original instance of {@link FSDataInputStream}.
   * @param fs instance of {@link FileSystem} in use.
   * @param bufferSize buffer size to be used.
   * @return the right {@link FSDataInputStream} as required.
   */
  private FSDataInputStream getFSDataInputStream(FSDataInputStream fsDataInputStream, FileSystem fs, int bufferSize) {
    if (fsDataInputStream.getWrappedStream() instanceof FSInputStream) {
      return new TimedFSDataInputStream(logFile.getPath(), new FSDataInputStream(
          new BufferedFSInputStream((FSInputStream) fsDataInputStream.getWrappedStream(), bufferSize)));
    }

    // fsDataInputStream.getWrappedStream() maybe a BufferedFSInputStream
    // need to wrap in another BufferedFSInputStream the make bufferSize work?
    return fsDataInputStream;
  }

  @Override
  public HoodieLogFile getLogFile() {
    return logFile;
  }

  /**
   * Close the inputstream if not closed when the JVM exits.
   */
  private void addShutDownHook() {
    shutdownThread = new Thread(() -> {
      try {
        close();
      } catch (Exception e) {
        LOG.warn("unable to close input stream for log file " + logFile, e);
        // fail silently for any sort of exception
      }
    });
    Runtime.getRuntime().addShutdownHook(shutdownThread);
  }

  // TODO : convert content and block length to long by using ByteBuffer, raw byte [] allows
  // for max of Integer size
  private HoodieLogBlock readBlock() throws IOException {

    int blocksize;
    int type;
    HoodieLogBlockType blockType = null;
    Map<HeaderMetadataType, String> header = null;

    try {
      // 1 Read the total size of the block
      blocksize = (int) inputStream.readLong();
    } catch (EOFException | CorruptedLogFileException e) {
      // An exception reading any of the above indicates a corrupt block
      // Create a corrupt block by finding the next MAGIC marker or EOF
      return createCorruptBlock();
    }

    // We may have had a crash which could have written this block partially
    // Skip blocksize in the stream and we should either find a sync marker (start of the next
    // block) or EOF. If we did not find either of it, then this block is a corrupted block.
    boolean isCorrupted = isBlockCorrupt(blocksize);
    if (isCorrupted) {
      return createCorruptBlock();
    }

    // 2. Read the version for this log format
    HoodieLogFormat.LogFormatVersion nextBlockVersion = readVersion();

    // 3. Read the block type for a log block
    if (nextBlockVersion.getVersion() != HoodieLogFormatVersion.DEFAULT_VERSION) {
      type = inputStream.readInt();

      ValidationUtils.checkArgument(type < HoodieLogBlockType.values().length, "Invalid block byte type found " + type);
      blockType = HoodieLogBlockType.values()[type];
    }

    // 4. Read the header for a log block, if present
    if (nextBlockVersion.hasHeader()) {
      header = HoodieLogBlock.getLogMetadata(inputStream);
    }

    int contentLength = blocksize;
    // 5. Read the content length for the content
    if (nextBlockVersion.getVersion() != HoodieLogFormatVersion.DEFAULT_VERSION) {
      contentLength = (int) inputStream.readLong();
    }

    // 6. Read the content or skip content based on IO vs Memory trade-off by client
    // TODO - have a max block size and reuse this buffer in the ByteBuffer
    // (hard to guess max block size for now)
    long contentPosition = inputStream.getPos();

    byte[] content = HoodieLogBlock.readOrSkipContent(inputStream, contentLength, readBlockLazily);

    // 7. Read footer if any
    Map<HeaderMetadataType, String> footer = null;
    if (nextBlockVersion.hasFooter()) {
      footer = HoodieLogBlock.getLogMetadata(inputStream);
    }

    // 8. Read log block length, if present. This acts as a reverse pointer when traversing a
    // log file in reverse      这在反向遍历日志文件时充当反向指针
    @SuppressWarnings("unused")
    long logBlockLength = 0;
    if (nextBlockVersion.hasLogBlockLength()) {
      logBlockLength = inputStream.readLong();
    }

    // 9. Read the log block end position in the log file
    long blockEndPos = inputStream.getPos();

    switch (Objects.requireNonNull(blockType)) {
      // based on type read the block
      case AVRO_DATA_BLOCK:
        if (nextBlockVersion.getVersion() == HoodieLogFormatVersion.DEFAULT_VERSION) {
          return HoodieAvroDataBlock.getBlock(content, readerSchema);
        } else {
          // 默认版本号1
          return new HoodieAvroDataBlock(logFile, inputStream, Option.ofNullable(content), readBlockLazily,
              contentPosition, contentLength, blockEndPos, readerSchema, header, footer, keyField);
        }
      case HFILE_DATA_BLOCK:
        return new HoodieHFileDataBlock(logFile, inputStream, Option.ofNullable(content), readBlockLazily,
            contentPosition, contentLength, blockEndPos, readerSchema,
            header, footer, enableInlineReading, keyField);
      case DELETE_BLOCK:
        return HoodieDeleteBlock.getBlock(logFile, inputStream, Option.ofNullable(content), readBlockLazily,
            contentPosition, contentLength, blockEndPos, header, footer);
      case COMMAND_BLOCK:
        return HoodieCommandBlock.getBlock(logFile, inputStream, Option.ofNullable(content), readBlockLazily,
            contentPosition, contentLength, blockEndPos, header, footer);
      default:
        throw new HoodieNotSupportedException("Unsupported Block " + blockType);
    }
  }

  private HoodieLogBlock createCorruptBlock() throws IOException {
    LOG.info("Log " + logFile + " has a corrupted block at " + inputStream.getPos());
    long currentPos = inputStream.getPos();
    //  通过读取magic 来获取下一个 block
    long nextBlockOffset = scanForNextAvailableBlockOffset();

    //  倒回到初始开始并读取损坏的字节直到 nextBlockOffset
    // Rewind to the initial start and read corrupted bytes till the nextBlockOffset
    inputStream.seek(currentPos);

    LOG.info("Next available block in " + logFile + " starts at " + nextBlockOffset);
    //  错误的block 大小
    int corruptedBlockSize = (int) (nextBlockOffset - currentPos);
    long contentPosition = inputStream.getPos();

    // corrupted Bytes
    byte[] corruptedBytes = HoodieLogBlock.readOrSkipContent(inputStream, corruptedBlockSize, readBlockLazily);

    return HoodieCorruptBlock.getBlock(logFile, inputStream, Option.ofNullable(corruptedBytes), readBlockLazily,
        contentPosition, corruptedBlockSize, corruptedBlockSize, new HashMap<>(), new HashMap<>());
  }

  /**
   *   对比前后两个 block size
   * @param blocksize
   * @return
   * @throws IOException
   */
  private boolean isBlockCorrupt(int blocksize) throws IOException {
    long currentPos = inputStream.getPos();
    try {
      inputStream.seek(currentPos + blocksize);
    } catch (EOFException e) {
      LOG.info("Found corrupted block in file " + logFile + " with block size(" + blocksize + ") running past EOF");
      // this is corrupt
      // This seek is required because contract of seek() is different for naked DFSInputStream vs BufferedFSInputStream
      // release-3.1.0-RC1/DFSInputStream.java#L1455
      // release-3.1.0-RC1/BufferedFSInputStream.java#L73
      inputStream.seek(currentPos);
      return true;
    }

    // check if the blocksize mentioned in the footer is the same as the header;    比较起始填充的blocksize 是否一致
    // by seeking back the length of a long
    // the backward seek does not incur additional IO as {@link org.apache.hadoop.hdfs.DFSInputStream#seek()}
    // only moves the index. actual IO happens on the next read operation
    //  移动到 footer 下的 block length 区域
    inputStream.seek(inputStream.getPos() - Long.BYTES);

    // Block size in the footer includes the magic header, which the header does not include.
    // So we have to shorten the footer block size by the size of magic hash
    long blockSizeFromFooter = inputStream.readLong() - magicBuffer.length;
    if (blocksize != blockSizeFromFooter) {
      LOG.info("Found corrupted block in file " + logFile + ". Header block size(" + blocksize
              + ") did not match the footer block size(" + blockSizeFromFooter + ")");
      inputStream.seek(currentPos);
      return true;
    }
    try {
      readMagic();
      // all good - either we found the sync marker or EOF. Reset position and continue
      return false;
    } catch (CorruptedLogFileException e) {
      // This is a corrupted block
      LOG.info("Found corrupted block in file " + logFile + ". No magic hash found right after footer block size entry");
      return true;
    } finally {
      inputStream.seek(currentPos);
    }
  }

  /**
   *  当前block error时， 读取下一个可用的block
   * @return
   * @throws IOException
   */
  private long scanForNextAvailableBlockOffset() throws IOException {
    // Make buffer large enough to scan through the file as quick as possible especially if it is on S3/GCS.
    byte[] dataBuf = new byte[BLOCK_SCAN_READ_BUFFER_SIZE];
    boolean eof = false;
    while (true) {
      long currentPos = inputStream.getPos();
      try {
        Arrays.fill(dataBuf, (byte) 0);
        // 读1M数据到缓存
        inputStream.readFully(dataBuf, 0, dataBuf.length);
      } catch (EOFException e) {
        //  数据流未填满
        eof = true;
      }
      // 通过 MAGIC 在dataBuf 的偏移量
      long pos = Bytes.indexOf(dataBuf, HoodieLogFormat.MAGIC);
      if (pos >= 0) {
        // 返回 magic 位置
        return currentPos + pos;
      }

      if (eof) {
        // 读取异常位置
        return inputStream.getPos();
      }
      // 当前未匹配到MAGIC，继续向下查找。- HoodieLogFormat.MAGIC.length 防止最后的自己为 #HUDI
      inputStream.seek(currentPos + dataBuf.length - HoodieLogFormat.MAGIC.length);
    }

  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      this.inputStream.close();
      if (null != shutdownThread) {
        Runtime.getRuntime().removeShutdownHook(shutdownThread);
      }
      closed = true;
    }
  }

  /*
   * hasNext is not idempotent. TODO - Fix this. It is okay for now - PR
   */
  @Override
  public boolean hasNext() {
    try {
      // 1. read Magic
      return readMagic();
    } catch (IOException e) {
      throw new HoodieIOException("IOException when reading logfile " + logFile, e);
    }
  }

  /**
   * Read log format version from log file.
   */
  private HoodieLogFormat.LogFormatVersion readVersion() throws IOException {
    return new HoodieLogFormatVersion(inputStream.readInt());
  }

  private boolean readMagic() throws IOException {
    try {
      boolean hasMagic = hasNextMagic();
      if (!hasMagic) {
        throw new CorruptedLogFileException(
            logFile + " could not be read. Did not find the magic bytes at the start of the block");
      }
      return hasMagic;
    } catch (EOFException e) {
      // We have reached the EOF
      return false;
    }
  }

  private boolean hasNextMagic() throws IOException {
    // 1. Read magic header from the start of the block
    inputStream.readFully(magicBuffer, 0, 6);
    return Arrays.equals(magicBuffer, HoodieLogFormat.MAGIC);
  }

  @Override
  public HoodieLogBlock next() {
    try {
      // hasNext() must be called before next()
      return readBlock();
    } catch (IOException io) {
      throw new HoodieIOException("IOException when reading logblock from log file " + logFile, io);
    }
  }

  /**
   * hasPrev is not idempotent.
   */
  @Override
  public boolean hasPrev() {
    try {
      if (!this.reverseReader) {
        throw new HoodieNotSupportedException("Reverse log reader has not been enabled");
      }
      reverseLogFilePosition = lastReverseLogFilePosition;
      reverseLogFilePosition -= Long.BYTES;
      lastReverseLogFilePosition = reverseLogFilePosition;
      inputStream.seek(reverseLogFilePosition);
    } catch (Exception e) {
      // Either reached EOF while reading backwards or an exception
      return false;
    }
    return true;
  }

  /**
   * This is a reverse iterator Note: At any point, an instance of HoodieLogFileReader should either iterate reverse
   * (prev) or forward (next). Doing both in the same instance is not supported WARNING : Every call to prev() should be
   * preceded with hasPrev()
   */
  @Override
  public HoodieLogBlock prev() throws IOException {

    if (!this.reverseReader) {
      throw new HoodieNotSupportedException("Reverse log reader has not been enabled");
    }
    long blockSize = inputStream.readLong();
    long blockEndPos = inputStream.getPos();
    // blocksize should read everything about a block including the length as well
    try {
      inputStream.seek(reverseLogFilePosition - blockSize);
    } catch (Exception e) {
      // this could be a corrupt block
      inputStream.seek(blockEndPos);
      throw new CorruptedLogFileException("Found possible corrupted block, cannot read log file in reverse, "
          + "fallback to forward reading of logfile");
    }
    boolean hasNext = hasNext();
    reverseLogFilePosition -= blockSize;
    lastReverseLogFilePosition = reverseLogFilePosition;
    return next();
  }

  /**
   * Reverse pointer, does not read the block. Return the current position of the log file (in reverse) If the pointer
   * (inputstream) is moved in any way, it is the job of the client of this class to seek/reset it back to the file
   * position returned from the method to expect correct results
   */
  public long moveToPrev() throws IOException {

    if (!this.reverseReader) {
      throw new HoodieNotSupportedException("Reverse log reader has not been enabled");
    }
    inputStream.seek(lastReverseLogFilePosition);
    long blockSize = inputStream.readLong();
    // blocksize should be everything about a block including the length as well
    inputStream.seek(reverseLogFilePosition - blockSize);
    reverseLogFilePosition -= blockSize;
    lastReverseLogFilePosition = reverseLogFilePosition;
    return reverseLogFilePosition;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove not supported for HoodieLogFileReader");
  }
}
