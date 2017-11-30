/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Key;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataMask;
import org.apache.orc.MemoryManager;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.writer.ColumnEncryption;
import org.apache.orc.impl.writer.EncryptionKey;
import org.apache.orc.impl.writer.MaskDescription;
import org.apache.orc.impl.writer.TreeWriter;
import org.apache.orc.impl.writer.WriterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import com.google.protobuf.ByteString;

/**
 * An ORC file writer. The file is divided into stripes, which is the natural
 * unit of work when reading. Each stripe is buffered in memory until the
 * memory reaches the stripe size and then it is written out broken down by
 * columns. Each column is written by a TreeWriter that is specific to that
 * type of column. TreeWriters may have children TreeWriters that handle the
 * sub-types. Each of the TreeWriters writes the column's data as a set of
 * streams.
 *
 * This class is unsynchronized like most Stream objects, so from the creation
 * of an OrcFile and all access to a single instance has to be from a single
 * thread.
 *
 * There are no known cases where these happen between different threads today.
 *
 * Caveat: the MemoryManager is created during WriterOptions create, that has
 * to be confined to a single thread as well.
 *
 */
public class WriterImpl implements Writer, MemoryManager.Callback {

  private static final Logger LOG = LoggerFactory.getLogger(WriterImpl.class);
  private static final HadoopShims SHIMS = HadoopShimsFactory.get();

  private static final int MIN_ROW_INDEX_STRIDE = 1000;

  private final Path path;
  private long adjustedStripeSize;
  private final int rowIndexStride;
  private final CompressionKind compress;
  private int bufferSize;
  private final TypeDescription schema;
  private final PhysicalWriter physicalWriter;
  private final OrcFile.WriterVersion writerVersion;

  private long rowCount = 0;
  private long rowsInStripe = 0;
  private long rawDataSize = 0;
  private int rowsInIndex = 0;
  private long lastFlushOffset = 0;
  private int stripesAtLastFlush = -1;
  private final List<OrcProto.StripeInformation> stripes =
    new ArrayList<>();
  private final OrcProto.Metadata.Builder fileMetadata =
      OrcProto.Metadata.newBuilder();
  private final Map<String, ByteString> userMetadata =
    new TreeMap<>();
  private final TreeWriter treeWriter;
  private final boolean buildIndex;
  private final MemoryManager memoryManager;
  private final OrcFile.Version version;
  private final Configuration conf;
  private final OrcFile.WriterCallback callback;
  private final OrcFile.WriterContext callbackContext;
  private final OrcFile.EncodingStrategy encodingStrategy;
  private final OrcFile.CompressionStrategy compressionStrategy;
  private final boolean[] bloomFilterColumns;
  private final double bloomFilterFpp;
  private final OrcFile.BloomFilterVersion bloomFilterVersion;
  private final boolean writeTimeZone;

  // encryption and masking
  private MaskDescription[] masks;
  private EncryptionKey[] keys;
  private final ColumnEncryption[] encryption;

  public WriterImpl(FileSystem fs,
                    Path path,
                    OrcFile.WriterOptions opts) throws IOException {
    this.path = path;
    this.conf = opts.getConfiguration();
    this.callback = opts.getCallback();
    this.schema = opts.getSchema();
    this.writerVersion = opts.getWriterVersion();
    bloomFilterVersion = opts.getBloomFilterVersion();
    if (callback != null) {
      callbackContext = new OrcFile.WriterContext(){

        @Override
        public Writer getWriter() {
          return WriterImpl.this;
        }
      };
    } else {
      callbackContext = null;
    }
    writeTimeZone = hasTimestamp(schema);
    this.adjustedStripeSize = opts.getStripeSize();
    this.version = opts.getVersion();
    this.encodingStrategy = opts.getEncodingStrategy();
    this.compressionStrategy = opts.getCompressionStrategy();
    this.compress = opts.getCompress();
    this.rowIndexStride = opts.getRowIndexStride();
    this.memoryManager = opts.getMemoryManager();
    buildIndex = rowIndexStride > 0;
    int numColumns = schema.getMaximumId() + 1;
    if (opts.isEnforceBufferSize()) {
      OutStream.assertBufferSizeValid(opts.getBufferSize());
      this.bufferSize = opts.getBufferSize();
    } else {
      this.bufferSize = getEstimatedBufferSize(adjustedStripeSize,
          numColumns, opts.getBufferSize());
    }
    if (version == OrcFile.Version.FUTURE) {
      throw new IllegalArgumentException("Can not write in a unknown version.");
    } else if (version == OrcFile.Version.UNSTABLE_PRE_2_0) {
      LOG.warn("ORC files written in " + version.getName() + " will not be" +
          " readable by other versions of the software. It is only for" +
          " developer testing.");
    }
    if (version == OrcFile.Version.V_0_11) {
      /* do not write bloom filters for ORC v11 */
      this.bloomFilterColumns = new boolean[schema.getMaximumId() + 1];
    } else {
      this.bloomFilterColumns =
          OrcUtils.includeColumns(opts.getBloomFilterColumns(), schema);
    }
    this.bloomFilterFpp = opts.getBloomFilterFpp();
    this.physicalWriter = opts.getPhysicalWriter() == null ?
        new PhysicalFsWriter(fs, path, opts) : opts.getPhysicalWriter();
    physicalWriter.writeHeader();
    // Do we have column encryption?
    List<OrcFile.EncryptionOption> encryptionOptions = opts.getEncryption();
    if (encryptionOptions.isEmpty()) {
      encryption = null;
      keys = new EncryptionKey[]{EncryptionKey.UNENCRYPTED};
    } else {
      encryption = new ColumnEncryption[schema.getMaximumId() + 1];
      setupEncryption(opts.getKeyProvider(), encryptionOptions);
    }

    treeWriter = TreeWriter.Factory.create(schema, new StreamFactory(), false);
    if (buildIndex && rowIndexStride < MIN_ROW_INDEX_STRIDE) {
      throw new IllegalArgumentException("Row stride must be at least " +
          MIN_ROW_INDEX_STRIDE);
    }
    // ensure that we are able to handle callbacks before we register ourselves
    memoryManager.addWriter(path, opts.getStripeSize(), this);
    LOG.info("ORC writer created for path: {} with stripeSize: {} blockSize: {}" +
        " compression: {} bufferSize: {}", path, adjustedStripeSize, opts.getBlockSize(),
        compress, bufferSize);
  }

  //@VisibleForTesting
  public static int getEstimatedBufferSize(long stripeSize, int numColumns,
                                           int bs) {
    // The worst case is that there are 2 big streams per a column and
    // we want to guarantee that each stream gets ~10 buffers.
    // This keeps buffers small enough that we don't get really small stripe
    // sizes.
    int estBufferSize = (int) (stripeSize / (20L * numColumns));
    estBufferSize = getClosestBufferSize(estBufferSize);
    return estBufferSize > bs ? bs : estBufferSize;
  }

  /**
   * Increase the buffer size for this writer.
   * This function is internal only and should only be called by the
   * ORC file merger.
   * @param newSize the new buffer size.
   */
  public void increaseCompressionSize(int newSize) {
    if (newSize > bufferSize) {
      bufferSize = newSize;
    }
  }

  private static int getClosestBufferSize(int estBufferSize) {
    final int kb4 = 4 * 1024;
    final int kb8 = 8 * 1024;
    final int kb16 = 16 * 1024;
    final int kb32 = 32 * 1024;
    final int kb64 = 64 * 1024;
    final int kb128 = 128 * 1024;
    final int kb256 = 256 * 1024;
    if (estBufferSize <= kb4) {
      return kb4;
    } else if (estBufferSize <= kb8) {
      return kb8;
    } else if (estBufferSize <= kb16) {
      return kb16;
    } else if (estBufferSize <= kb32) {
      return kb32;
    } else if (estBufferSize <= kb64) {
      return kb64;
    } else if (estBufferSize <= kb128) {
      return kb128;
    } else {
      return kb256;
    }
  }

  public static CompressionCodec createCodec(CompressionKind kind) {
    switch (kind) {
      case NONE:
        return null;
      case ZLIB:
        return new ZlibCodec();
      case SNAPPY:
        return new SnappyCodec();
      case LZO:
        return new AircompressorCodec(new LzoCompressor(),
            new LzoDecompressor());
      case LZ4:
        return new AircompressorCodec(new Lz4Compressor(),
            new Lz4Decompressor());
      default:
        throw new IllegalArgumentException("Unknown compression codec: " +
            kind);
    }
  }

  @Override
  public boolean checkMemory(double newScale) throws IOException {
    long limit = Math.round(adjustedStripeSize * newScale);
    long size = treeWriter.estimateMemory();
    if (LOG.isDebugEnabled()) {
      LOG.debug("ORC writer " + physicalWriter + " size = " + size +
          " limit = " + limit);
    }
    if (size > limit) {
      flushStripe();
      return true;
    }
    return false;
  }

  /**
   * Interface from the Writer to the TreeWriters. This limits the visibility
   * that the TreeWriters have into the Writer.
   */
  private class StreamFactory implements WriterContext {

    /**
     * Create a stream to store part of a column.
     * @param column the column id for the stream
     * @param kind the kind of stream
     * @return The output outStream that the section needs to be written to.
     */
    public OutStream createStream(int column,
                                  OrcProto.Stream.Kind kind
                                  ) throws IOException {
      final StreamName name = new StreamName(column, kind);
      CompressionCodec codec = getCustomizedCodec(kind);

      return new OutStream(name, bufferSize, codec, null, null,
          physicalWriter.createDataStream(name));
    }

    /**
     * Get the stride rate of the row index.
     */
    public int getRowIndexStride() {
      return rowIndexStride;
    }

    /**
     * Should be building the row index.
     * @return true if we are building the index
     */
    public boolean buildIndex() {
      return buildIndex;
    }

    /**
     * Is the ORC file compressed?
     * @return are the streams compressed
     */
    public boolean isCompressed() {
      return physicalWriter.getCompressionCodec() != null;
    }

    /**
     * Get the encoding strategy to use.
     * @return encoding strategy
     */
    public OrcFile.EncodingStrategy getEncodingStrategy() {
      return encodingStrategy;
    }

    /**
     * Get the bloom filter columns
     * @return bloom filter columns
     */
    public boolean[] getBloomFilterColumns() {
      return bloomFilterColumns;
    }

    /**
     * Get bloom filter false positive percentage.
     * @return fpp
     */
    public double getBloomFilterFPP() {
      return bloomFilterFpp;
    }

    /**
     * Get the writer's configuration.
     * @return configuration
     */
    public Configuration getConfiguration() {
      return conf;
    }

    /**
     * Get the version of the file to write.
     */
    public OrcFile.Version getVersion() {
      return version;
    }

    public OrcFile.BloomFilterVersion getBloomFilterVersion() {
      return bloomFilterVersion;
    }

    public void writeIndex(StreamName name,
                           OrcProto.RowIndex.Builder index) throws IOException {
      physicalWriter.writeIndex(name, index, getCustomizedCodec(name.getKind()),
          null, null);
    }

    public void writeBloomFilter(StreamName name,
                                 OrcProto.BloomFilterIndex.Builder bloom
                                 ) throws IOException {
      physicalWriter.writeBloomFilter(name, bloom,
          getCustomizedCodec(name.getKind()), null, null);
    }

    @Override
    public int getBufferSize() {
      return bufferSize;
    }

    @Override
    public PhysicalWriter.OutputReceiver getReceiver(StreamName name
                                                     ) throws IOException {
      return physicalWriter.createDataStream(name);
    }

    @Override
    public CompressionCodec getCustomizedCodec(OrcProto.Stream.Kind kind) {
      // TODO: modify may create a new codec here. We want to end() it when the stream is closed,
      //       but at this point there's no close() for the stream.
      CompressionCodec result = physicalWriter.getCompressionCodec();
      if (result != null) {
        switch (kind) {
          case BLOOM_FILTER:
          case DATA:
          case DICTIONARY_DATA:
          case BLOOM_FILTER_UTF8:
            if (compressionStrategy == OrcFile.CompressionStrategy.SPEED) {
              result = result.modify(EnumSet.of(CompressionCodec.Modifier.FAST,
                  CompressionCodec.Modifier.TEXT));
            } else {
              result = result.modify(EnumSet.of(CompressionCodec.Modifier.DEFAULT,
                  CompressionCodec.Modifier.TEXT));
            }
            break;
          case LENGTH:
          case DICTIONARY_COUNT:
          case PRESENT:
          case ROW_INDEX:
          case SECONDARY:
            // easily compressed using the fastest modes
            result = result.modify(EnumSet.of(CompressionCodec.Modifier.FASTEST,
                CompressionCodec.Modifier.BINARY));
            break;
          default:
            LOG.info("Missing ORC compression modifiers for " + kind);
            break;
        }
      }
      return result;
    }

    @Override
    public ColumnEncryption getEncryption(int columnId) {
      return encryption == null ? null : encryption[columnId];
    }

    @Override
    public EncryptionKey getKey() {
      return EncryptionKey.UNENCRYPTED;
    }

    public DataMask createMask(int maskId, TypeDescription schema) {
      return masks[maskId].create(schema);
    }

    @Override
    public PhysicalWriter getPhysicalWriter() {
      return physicalWriter;
    }

    @Override
    public void setEncoding(int column, OrcProto.ColumnEncoding encoding) {
      physicalWriter.setColumnEncoding(column, getKey(), encoding);
    }

    @Override
    public void setStripeStatistics(int column, OrcProto.ColumnStatistics stats) {
      physicalWriter.appendStripeStatistics(column, getKey(), stats);
    }
  }


  private static void writeTypes(OrcProto.Footer.Builder builder,
                                 TypeDescription schema) {
    builder.addAllTypes(OrcUtils.getOrcTypes(schema));
  }

  private void createRowIndexEntry() throws IOException {
    treeWriter.createRowIndexEntry();
    rowsInIndex = 0;
  }

  private void flushStripe() throws IOException {
    if (buildIndex && rowsInIndex != 0) {
      createRowIndexEntry();
    }
    if (rowsInStripe != 0) {
      if (callback != null) {
        callback.preStripeWrite(callbackContext);
      }
      // finalize the data for the stripe
      int requiredIndexEntries = rowIndexStride == 0 ? 0 :
          (int) ((rowsInStripe + rowIndexStride - 1) / rowIndexStride);
      OrcProto.StripeFooter.Builder builder =
          OrcProto.StripeFooter.newBuilder();
      if (writeTimeZone) {
        builder.setWriterTimezone(TimeZone.getDefault().getID());
      }
      OrcProto.StripeStatistics.Builder stats =
          OrcProto.StripeStatistics.newBuilder();
      treeWriter.writeStripe(requiredIndexEntries);
      fileMetadata.addStripeStats(stats.build());
      OrcProto.StripeInformation.Builder dirEntry =
          OrcProto.StripeInformation.newBuilder()
              .setNumberOfRows(rowsInStripe);
      physicalWriter.finalizeStripe(builder, dirEntry);
      stripes.add(dirEntry.build());
      rowCount += rowsInStripe;
      rowsInStripe = 0;
    }
  }

  private long computeRawDataSize() {
    return treeWriter.getRawDataSize();
  }

  private OrcProto.CompressionKind writeCompressionKind(CompressionKind kind) {
    switch (kind) {
      case NONE: return OrcProto.CompressionKind.NONE;
      case ZLIB: return OrcProto.CompressionKind.ZLIB;
      case SNAPPY: return OrcProto.CompressionKind.SNAPPY;
      case LZO: return OrcProto.CompressionKind.LZO;
      case LZ4: return OrcProto.CompressionKind.LZ4;
      default:
        throw new IllegalArgumentException("Unknown compression " + kind);
    }
  }

  private OrcProto.FileStatistics.Builder[] writeFileStatistics(
      TreeWriter writer) throws IOException {
    OrcProto.FileStatistics.Builder[] result =
        new OrcProto.FileStatistics.Builder[keys.length];
    for (int i=0; i < result.length; ++i) {
      result[i] = OrcProto.FileStatistics.newBuilder();
    }
    writer.writeFileStatistics(result);
    return result;
  }

  private void writeMetadata() throws IOException {
    physicalWriter.writeFileMetadata(fileMetadata);
  }

  private long writePostScript() throws IOException {
    OrcProto.PostScript.Builder builder =
        OrcProto.PostScript.newBuilder()
            .setCompression(writeCompressionKind(compress))
            .setMagic(OrcFile.MAGIC)
            .addVersion(version.getMajor())
            .addVersion(version.getMinor())
            .setWriterVersion(writerVersion.getId());
    if (compress != CompressionKind.NONE) {
      builder.setCompressionBlockSize(bufferSize);
    }
    return physicalWriter.writePostScript(builder);
  }

  private static final class BufferCollector implements PhysicalWriter.OutputReceiver {
    private final ByteString.Output output = ByteString.newOutput();

    @Override
    public void output(ByteBuffer buffer) {
      output.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
    }

    @Override
    public void suppress() {
      output.reset();
    }

    ByteString toByteString() {
      return output.toByteString();
    }
  }

  private OrcProto.EncryptionKey writeKey(EncryptionKey key) {
    OrcProto.EncryptionKey.Builder result = OrcProto.EncryptionKey.newBuilder();
    HadoopShims.KeyMetadata meta = key.getMetadata();
    result.setKeyName(meta.getKeyName());
    result.setKeyVersion(meta.getVersion());
    result.setAlgorithm(OrcProto.EncryptionAlgorithm.valueOf(
        meta.getAlgorithm().getSerialization()));
    result.addKeyIv(ByteString.copyFrom(key.getKeyIv()));
    for(ColumnEncryption column: key.getRoots()) {
      result.addRootId(column.getRoot());
      result.addUnencryptedMask(column.getUnencryptedMask());
    }
    return result.build();
  }

  private ByteString encryptFileStats(CompressionCodec codec,
                                      EncryptionKey key,
                                      OrcProto.FileStatistics stats
                                      ) throws IOException {
    BufferCollector buffer = new BufferCollector();
    OutStream stream = new OutStream(
        new StreamName(0, OrcProto.Stream.Kind.FILE_STATISTICS, key.getId()),
        bufferSize, codec, key.getAlgorithm(), key.getFileStatsKey(),
        buffer);
    stats.writeTo(stream);
    stream.flush();
    return buffer.toByteString();
  }

  private void writeEncryptionFooter(OrcProto.Footer.Builder builder,
                                     OrcProto.FileStatistics.Builder[] stats
                                     ) throws IOException {
    OrcProto.Encryption.Builder encrypt = OrcProto.Encryption.newBuilder();
    for(MaskDescription mask: masks) {
      OrcProto.DataMask.Builder maskBuilder = OrcProto.DataMask.newBuilder();
      maskBuilder.setName(mask.getStyle());
      String[] params = mask.getParameters();
      if (params != null) {
        for(String param: params) {
          maskBuilder.addMaskParameters(param);
        }
      }
      encrypt.addMask(maskBuilder);
    }
    for(EncryptionKey key: keys) {
      if (key != EncryptionKey.UNENCRYPTED) {
        encrypt.addKey(writeKey(key));
      }
    }
    CompressionCodec codec = getCompressionCodec();
    for(int k = 1; k < stats.length; ++k) {
      encrypt.addFileStatistics(encryptFileStats(codec, keys[k],
          stats[k].build()));
    }
    builder.setEncryption(encrypt);
  }

  private long writeFooter() throws IOException {
    writeMetadata();
    OrcProto.Footer.Builder builder = OrcProto.Footer.newBuilder();
    builder.setNumberOfRows(rowCount);
    builder.setRowIndexStride(rowIndexStride);
    rawDataSize = computeRawDataSize();
    // serialize the types
    writeTypes(builder, schema);
    // add the stripe information
    for(OrcProto.StripeInformation stripe: stripes) {
      builder.addStripes(stripe);
    }
    // add the column statistics
    OrcProto.FileStatistics.Builder[] fileStatistics =
        writeFileStatistics(treeWriter);
    // add the unencrypted stats
    for(OrcProto.ColumnStatistics stat: fileStatistics[0].getColumnList()) {
      builder.addStatistics(stat);
    }
    // add all of the user metadata
    for(Map.Entry<String, ByteString> entry: userMetadata.entrySet()) {
      builder.addMetadata(OrcProto.UserMetadataItem.newBuilder()
        .setName(entry.getKey()).setValue(entry.getValue()));
    }
    if (encryption != null) {
      writeEncryptionFooter(builder, fileStatistics);
    }
    builder.setWriter(OrcFile.WriterImplementation.ORC_JAVA.getId());
    physicalWriter.writeFileFooter(builder);
    return writePostScript();
  }

  @Override
  public TypeDescription getSchema() {
    return schema;
  }

  @Override
  public void addUserMetadata(String name, ByteBuffer value) {
    userMetadata.put(name, ByteString.copyFrom(value));
  }

  @Override
  public void addRowBatch(VectorizedRowBatch batch) throws IOException {
    if (buildIndex) {
      // Batch the writes up to the rowIndexStride so that we can get the
      // right size indexes.
      int posn = 0;
      while (posn < batch.size) {
        int chunkSize = Math.min(batch.size - posn,
            rowIndexStride - rowsInIndex);
        treeWriter.writeRootBatch(batch, posn, chunkSize);
        posn += chunkSize;
        rowsInIndex += chunkSize;
        rowsInStripe += chunkSize;
        if (rowsInIndex >= rowIndexStride) {
          createRowIndexEntry();
        }
      }
    } else {
      rowsInStripe += batch.size;
      treeWriter.writeRootBatch(batch, 0, batch.size);
    }
    memoryManager.addedRow(batch.size);
  }

  @Override
  public void close() throws IOException {
    if (callback != null) {
      callback.preFooterWrite(callbackContext);
    }
    // remove us from the memory manager so that we don't get any callbacks
    memoryManager.removeWriter(path);
    // actually close the file
    flushStripe();
    lastFlushOffset = writeFooter();
    physicalWriter.close();
  }

  /**
   * Raw data size will be compute when writing the file footer. Hence raw data
   * size value will be available only after closing the writer.
   */
  @Override
  public long getRawDataSize() {
    return rawDataSize;
  }

  /**
   * Row count gets updated when flushing the stripes. To get accurate row
   * count call this method after writer is closed.
   */
  @Override
  public long getNumberOfRows() {
    return rowCount;
  }

  @Override
  public long writeIntermediateFooter() throws IOException {
    // flush any buffered rows
    flushStripe();
    // write a footer
    if (stripesAtLastFlush != stripes.size()) {
      if (callback != null) {
        callback.preFooterWrite(callbackContext);
      }
      lastFlushOffset = writeFooter();
      stripesAtLastFlush = stripes.size();
      physicalWriter.flush();
    }
    return lastFlushOffset;
  }

  static void checkArgument(boolean expression, String message) {
    if (!expression) {
      throw new IllegalArgumentException(message);
    }
  }

  @Override
  public void appendStripe(byte[] stripe, int offset, int length,
      StripeInformation stripeInfo,
      OrcProto.StripeStatistics stripeStatistics) throws IOException {
    checkArgument(stripe != null, "Stripe must not be null");
    checkArgument(length <= stripe.length,
        "Specified length must not be greater specified array length");
    checkArgument(stripeInfo != null, "Stripe information must not be null");
    checkArgument(stripeStatistics != null,
        "Stripe statistics must not be null");

    rowsInStripe = stripeInfo.getNumberOfRows();
    // update stripe information
    OrcProto.StripeInformation.Builder dirEntry = OrcProto.StripeInformation
        .newBuilder()
        .setNumberOfRows(rowsInStripe)
        .setIndexLength(stripeInfo.getIndexLength())
        .setDataLength(stripeInfo.getDataLength())
        .setFooterLength(stripeInfo.getFooterLength());
    physicalWriter.appendRawStripe(ByteBuffer.wrap(stripe, offset, length),
        dirEntry);

    // since we have already written the stripe, just update stripe statistics
    treeWriter.updateFileStatistics(stripeStatistics);
    fileMetadata.addStripeStats(stripeStatistics);

    stripes.add(dirEntry.build());

    // reset it after writing the stripe
    rowCount += rowsInStripe;
    rowsInStripe = 0;
  }

  @Override
  public void appendUserMetadata(List<OrcProto.UserMetadataItem> userMetadata) {
    if (userMetadata != null) {
      for (OrcProto.UserMetadataItem item : userMetadata) {
        this.userMetadata.put(item.getName(), item.getValue());
      }
    }
  }

  @Override
  public ColumnStatistics[] getStatistics() throws IOException {
    // get the column statistics
    OrcProto.FileStatistics.Builder[] stats = writeFileStatistics(treeWriter);
    return ReaderImpl.deserializeStats(stats[0].getColumnList());
  }

  public CompressionCodec getCompressionCodec() {
    return physicalWriter.getCompressionCodec();
  }

  private static boolean hasTimestamp(TypeDescription schema) {
    if (schema.getCategory() == TypeDescription.Category.TIMESTAMP) {
      return true;
    }
    List<TypeDescription> children = schema.getChildren();
    if (children != null) {
      for (TypeDescription child : children) {
        if (hasTimestamp(child)) {
          return true;
        }
      }
    }
    return false;
  }

  static EncryptionKey getKey(Map<String,EncryptionKey> keys,
                              String keyName,
                              HadoopShims.KeyProvider provider) throws IOException {
    EncryptionKey result = keys.get(keyName);
    if (result == null) {
      result = new EncryptionKey(provider, keyName, keys.size() + 1);
      keys.put(keyName, result);
    }
    return result;
  }

  int getMask(Map<MaskDescription,Integer> masks,
              OrcFile.EncryptionOption opt) {
    MaskDescription mask = new MaskDescription(opt.getMask(),
        opt.getMaskParameters());

    Integer id = masks.get(mask);
    if (id == null) {
      id = masks.size();
      masks.put(mask, id);
    }
    return id;
  }

  /**
   * Iterate through the encryption options given by the user and set up
   * our data structures.
   * @param provider the KeyProvider to use to generate keys
   * @param options the options from the user
   */
  void setupEncryption(HadoopShims.KeyProvider provider,
                       List<OrcFile.EncryptionOption> options) throws IOException {
    if (provider == null) {
      provider = SHIMS.getKeyProvider(conf);
    }
    Map<String,EncryptionKey> keys = new HashMap<>();
    Map<MaskDescription, Integer> masks = new HashMap<>();
    // fill out the primary encryption keys
    for(OrcFile.EncryptionOption option: options) {
      EncryptionKey key = getKey(keys, option.getKeyName(), provider);
      int mask = getMask(masks, option);
      int root = option.getColumnId();
      HadoopShims.KeyMetadata metadata = key.getMetadata();
      byte[] columnIv = CryptoUtils.createIvForPassword(metadata.getAlgorithm(),
          key.getKeyIv(), root);
      Key material = provider.getLocalKey(metadata, columnIv);
      ColumnEncryption column = new ColumnEncryption(key, root, mask, material);
      key.addRoot(column);
      encryption[root] = column;
    }
    // Now that we have de-duped the keys and masks, make the arrays
    this.keys = new EncryptionKey[keys.size() + 1];
    this.keys[0] = EncryptionKey.UNENCRYPTED;
    for(EncryptionKey key: keys.values()) {
      this.keys[key.getId()] = key;
    }
    this.masks = new MaskDescription[masks.size()];
    for(Map.Entry<MaskDescription,Integer> mask: masks.entrySet()) {
      this.masks[mask.getValue()] = mask.getKey();
    }
  }
}
