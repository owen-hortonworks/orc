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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.Key;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.protobuf.CodedOutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.writer.ColumnEncryption;
import org.apache.orc.impl.writer.EncryptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PhysicalFsWriter implements PhysicalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PhysicalFsWriter.class);

  private static final int HDFS_BUFFER_SIZE = 256 * 1024;
  private static final HadoopShims shims = HadoopShimsFactory.get();

  private final FSDataOutputStream rawWriter;
  private final DirectStream rawStream;
  private final TypeDescription schema;
  // the compressed metadata information outStream
  private OutStream stripeFooterWriter = null;
  // a protobuf outStream around streamFactory
  private CodedOutputStream stripeFooterStream = null;

  private final Path path;
  private final long blockSize;
  private final int bufferSize;
  private final double paddingTolerance;
  private final long defaultStripeSize;
  private final CompressionKind compress;
  private final CompressionCodec codec;
  private final boolean addBlockPadding;

  // the streams that make up the current stripe
  private final Map<StreamName, BufferedStream> streams =
    new TreeMap<>();

  private long adjustedStripeSize;
  private long headerLength;
  private long stripeStart;
  private long blockStart;
  private int metadataLength;
  private int footerLength;

  private Map<EncryptionKey, OrcProto.ColumnEncoding[]> columnEncoding =
      new HashMap<>();
  private Map<Enc>

  public PhysicalFsWriter(FileSystem fs,
                          Path path,
                          OrcFile.WriterOptions opts) throws IOException {
    this.path = path;
    this.defaultStripeSize = this.adjustedStripeSize = opts.getStripeSize();
    this.addBlockPadding = opts.getBlockPadding();
    this.schema = opts.getSchema();
    if (opts.isEnforceBufferSize()) {
      this.bufferSize = opts.getBufferSize();
    } else {
      this.bufferSize = WriterImpl.getEstimatedBufferSize(defaultStripeSize,
          opts.getSchema().getMaximumId() + 1,
          opts.getBufferSize());
    }
    this.compress = opts.getCompress();
    this.paddingTolerance = opts.getPaddingTolerance();
    this.blockSize = opts.getBlockSize();
    LOG.info("ORC writer created for path: {} with stripeSize: {} blockSize: {}" +
        " compression: {} bufferSize: {}", path, defaultStripeSize, blockSize,
        compress, bufferSize);
    rawWriter = fs.create(path, opts.getOverwrite(), HDFS_BUFFER_SIZE,
        fs.getDefaultReplication(path), blockSize);
    codec = OrcCodecPool.getCodec(compress);
    rawStream = new DirectStream(rawWriter);
    stripeFooterWriter =
        new OutStream(new StreamName(0, OrcProto.Stream.Kind.STRIPE_FOOTER),
            bufferSize, codec, null, null, rawStream);
    stripeFooterStream = CodedOutputStream.newInstance(stripeFooterWriter);
  }

  @Override
  public CompressionCodec getCompressionCodec() {
    return codec;
  }

  private void padStripe(long indexSize, long dataSize, int footerSize) throws IOException {
    this.stripeStart = rawWriter.getPos();
    final long currentStripeSize = indexSize + dataSize + footerSize;
    final long available = blockSize - (stripeStart - blockStart);
    final long overflow = currentStripeSize - adjustedStripeSize;
    final float availRatio = (float) available / (float) defaultStripeSize;

    if (availRatio > 0.0f && availRatio < 1.0f
        && availRatio > paddingTolerance) {
      // adjust default stripe size to fit into remaining space, also adjust
      // the next stripe for correction based on the current stripe size
      // and user specified padding tolerance. Since stripe size can overflow
      // the default stripe size we should apply this correction to avoid
      // writing portion of last stripe to next hdfs block.
      double correction = overflow > 0 ? (double) overflow
          / (double) adjustedStripeSize : 0.0;

      // correction should not be greater than user specified padding
      // tolerance
      correction = correction > paddingTolerance ? paddingTolerance
          : correction;

      // adjust next stripe size based on current stripe estimate correction
      adjustedStripeSize = (long) ((1.0f - correction) * (availRatio * defaultStripeSize));
    } else if (availRatio >= 1.0) {
      adjustedStripeSize = defaultStripeSize;
    }

    if (addBlockPadding) {
      if (availRatio < paddingTolerance) {
        long padding = blockSize - (stripeStart - blockStart);
        LOG.info(String.format("Padding ORC by %d bytes (<=  %.2f * %d)",
            padding, availRatio, defaultStripeSize));
        stripeStart += shims.padStreamToBlock(rawWriter, padding);
        blockStart = stripeStart; // new block
        adjustedStripeSize = defaultStripeSize;
      }
    } else if (currentStripeSize < blockSize
        && (stripeStart - blockStart) + currentStripeSize > blockSize) {
      // even if you don't intend to pad, reset the default stripe size when crossing a
      // block boundary
      adjustedStripeSize = defaultStripeSize;
      blockStart = stripeStart + (stripeStart + currentStripeSize) % blockSize;
    }
  }

  /**
   * An output receiver that writes the ByteBuffers to the output stream
   * as they are received.
   */
  private static class DirectStream implements OutputReceiver {
    private final FSDataOutputStream output;

    DirectStream(FSDataOutputStream output) {
      this.output = output;
    }

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      output.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
    }

    @Override
    public void suppress() {
      throw new UnsupportedOperationException("Can't suppress direct stream");
    }
  }

  private void writeStripeFooter(OrcProto.StripeFooter footer,
                                 long dataSize,
                                 long indexSize,
                                 OrcProto.StripeInformation.Builder dirEntry) throws IOException {
    footer.writeTo(stripeFooterStream);
    stripeFooterStream.flush();
    stripeFooterWriter.flush();
    dirEntry.setOffset(stripeStart);
    dirEntry.setFooterLength(rawWriter.getPos() - stripeStart - dataSize - indexSize);
  }

  @Override
  public void writeFileMetadata(OrcProto.Metadata.Builder builder) throws IOException {
    long startPosn = rawWriter.getPos();
    OutStream writer =
        new OutStream(new StreamName(0, OrcProto.Stream.Kind.STRIPE_STATISTICS),
            bufferSize, codec, null, null, rawStream);
    OrcProto.Metadata metadata = builder.build();
    metadata.writeTo(writer);
    writer.flush();
    this.metadataLength = (int) (rawWriter.getPos() - startPosn);
  }

  @Override
  public void writeFileFooter(OrcProto.Footer.Builder builder) throws IOException {
    long bodyLength = rawWriter.getPos() - metadataLength;
    builder.setContentLength(bodyLength);
    builder.setHeaderLength(headerLength);
    long startPosn = rawWriter.getPos();
    OrcProto.Footer footer = builder.build();
    OutStream writer =
        new OutStream(new StreamName(0, OrcProto.Stream.Kind.FILE_FOOTER),
            bufferSize, codec, null, null, rawStream);
    footer.writeTo(writer);
    writer.flush();
    this.footerLength = (int) (rawWriter.getPos() - startPosn);
  }

  @Override
  public long writePostScript(OrcProto.PostScript.Builder builder) throws IOException {
    builder.setFooterLength(footerLength);
    builder.setMetadataLength(metadataLength);
    OrcProto.PostScript ps = builder.build();
    // need to write this uncompressed
    long startPosn = rawWriter.getPos();
    ps.writeTo(rawWriter);
    long length = rawWriter.getPos() - startPosn;
    if (length > 255) {
      throw new IllegalArgumentException("PostScript too large at " + length);
    }
    rawWriter.writeByte((int)length);
    return rawWriter.getPos();
  }

  @Override
  public void close() throws IOException {
    OrcCodecPool.returnCodec(compress, codec);
    rawWriter.close();
  }

  @Override
  public void flush() throws IOException {
    rawWriter.hflush();
  }

  @Override
  public void appendRawStripe(ByteBuffer buffer,
      OrcProto.StripeInformation.Builder dirEntry) throws IOException {
    long start = rawWriter.getPos();
    int length = buffer.remaining();
    long availBlockSpace = blockSize - (start % blockSize);

    // see if stripe can fit in the current hdfs block, else pad the remaining
    // space in the block
    if (length < blockSize && length > availBlockSpace &&
        addBlockPadding) {
      byte[] pad = new byte[(int) Math.min(HDFS_BUFFER_SIZE, availBlockSpace)];
      LOG.info(String.format("Padding ORC by %d bytes while merging..",
          availBlockSpace));
      start += availBlockSpace;
      while (availBlockSpace > 0) {
        int writeLen = (int) Math.min(availBlockSpace, pad.length);
        rawWriter.write(pad, 0, writeLen);
        availBlockSpace -= writeLen;
      }
    }
    rawWriter.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
        length);
    dirEntry.setOffset(start);
  }


  /**
   * This class is used to hold the contents of streams as they are buffered.
   * The TreeWriters write to the outStream and the codec compresses the
   * data as buffers fill up and stores them in the output list. When the
   * stripe is being written, the whole stream is written to the file.
   */
  private static final class BufferedStream implements OutputReceiver {
    private boolean isSuppressed = false;
    private final List<ByteBuffer> output = new ArrayList<>();

    @Override
    public void output(ByteBuffer buffer) {
      if (!isSuppressed) {
        output.add(buffer);
      }
    }

    public void suppress() {
      isSuppressed = true;
      output.clear();
    }

    /**
     * Write any saved buffers to the OutputStream if needed, and clears all the
     * buffers.
     */
    void spillToDiskAndClear(FSDataOutputStream raw
                                       ) throws IOException {
      if (!isSuppressed) {
        for (ByteBuffer buffer: output) {
          raw.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
            buffer.remaining());
        }
        output.clear();
      }
      isSuppressed = false;
    }

    /**
     * Get the number of bytes that will be written to the output.
     *
     * Assumes the stream writing into this receiver has already been flushed.
     * @return number of bytes
     */
    public long getOutputSize() {
      long result = 0;
      for (ByteBuffer buffer: output) {
        result += buffer.remaining();
      }
      return result;
    }
  }

  @Override
  public void finalizeStripe(OrcProto.StripeFooter.Builder footerBuilder,
                             OrcProto.StripeInformation.Builder dirEntry
                             ) throws IOException {
    long indexSize = 0;
    long dataSize = 0;
    long encryptedIndexSize = 0;
    long encryptedDataSize = 0;
    int lastKey = StreamName.UNENCRYPTED;
    OrcProto.StripeEncryption.Builder encryption = null;
    OrcProto.StripeFooter.Builder encryptedFooter = null;
    for (Map.Entry<StreamName, BufferedStream> pair: streams.entrySet()) {
      BufferedStream receiver = pair.getValue();
      if (!receiver.isSuppressed) {
        long streamSize = receiver.getOutputSize();
        StreamName name = pair.getKey();
        OrcProto.Stream stream = OrcProto.Stream.newBuilder()
            .setColumn(name.getColumn())
            .setKind(name.getKind())
            .setLength(streamSize).build();
        if (name.isEncrypted()) {
          int currentKey = name.getKey();
          if (lastKey != currentKey) {
            if (lastKey != StreamName.UNENCRYPTED) {
              dirEntry.addEncryptedDataLength(encryptedDataSize)
                  .addEncryptedIndexLength(encryptedIndexSize);
              encryption.addFooter(encryptedFooter);
            } else {
              encryption = footerBuilder.getEncryptionBuilder();
            }
            encryptedFooter = encryption.getFooterBuilder(currentKey);
            lastKey = currentKey;
          }
          encryptedFooter.addStreams(stream);
        } else {
          footerBuilder.addStreams(stream);
        }
        switch (name.getArea()) {
          case DATA:
            dataSize += streamSize;
            break;
          case INDEX:
            indexSize += streamSize;
            break;
          case ENCRYPTED_DATA:
            encryptedDataSize += streamSize;
            break;
          case ENCRYPTED_INDEX:
            encryptedIndexSize += streamSize;
            break;
          default:
            throw new IllegalArgumentException("Unknown area " + name.getArea());
        }
      }
    }
    dirEntry
        .setIndexLength(indexSize)
        .setDataLength(dataSize);

    if (encryption != null) {
      encryption.addFooter(encryptedFooter);
      footerBuilder.setEncryption(encryption);
      dirEntry
          .addEncryptedIndexLength(encryptedIndexSize)
          .addEncryptedDataLength(encryptedDataSize);
    }

    OrcProto.StripeFooter footer = footerBuilder.build();
    // Do we need to pad the file so the stripe doesn't straddle a block boundary?
    padStripe(indexSize + encryptedIndexSize, dataSize + encryptedDataSize,
        footer.getSerializedSize());

    // write out the unencrypted streams
    for (Map.Entry<StreamName, BufferedStream> pair : streams.entrySet()) {
      if (!pair.getKey().isEncrypted()) {
        pair.getValue().spillToDiskAndClear(rawWriter);
      }
    }

    // Write out the footer.
    writeStripeFooter(footer, dataSize, indexSize, dirEntry);

    // write out the encrypted streams
    for (Map.Entry<StreamName, BufferedStream> pair : streams.entrySet()) {
      if (pair.getKey().isEncrypted()) {
        pair.getValue().spillToDiskAndClear(rawWriter);
      }
    }
  }

  @Override
  public void writeHeader() throws IOException {
    rawWriter.writeBytes(OrcFile.MAGIC);
    headerLength = rawWriter.getPos();
  }

  @Override
  public BufferedStream createDataStream(StreamName name) {
    BufferedStream result = streams.get(name);
    if (result == null) {
      result = new BufferedStream();
      streams.put(name, result);
    }
    return result;
  }

  @Override
  public void writeIndex(StreamName name,
                         OrcProto.RowIndex.Builder index,
                         CompressionCodec codec,
                         EncryptionAlgorithm encryption,
                         Key material) throws IOException {
    OutputStream stream = new OutStream(name, bufferSize, codec,
        encryption, material, createDataStream(name));
    index.build().writeTo(stream);
    stream.flush();
  }

  @Override
  public void writeBloomFilter(StreamName name,
                               OrcProto.BloomFilterIndex.Builder bloom,
                               CompressionCodec codec,
                               EncryptionAlgorithm encryption,
                               Key material) throws IOException {
    OutputStream stream = new OutStream(name, bufferSize, codec,
        encryption, material, createDataStream(name));
    bloom.build().writeTo(stream);
    stream.flush();
  }

  @Override
  public void setColumnEncoding(int column, EncryptionKey key, OrcProto.ColumnEncoding encoding) {

  }

  @Override
  public void appendStripeStatistics(int column, EncryptionKey key, OrcProto.ColumnStatistics stats) {

  }

  @Override
  public String toString() {
    return path.toString();
  }
}
