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

package org.apache.orc.impl.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionCodec;
import org.apache.orc.DataMask;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.StreamName;

import java.io.IOException;

/**
 * TreeWriter that handles column encryption.
 * We create a TreeWriter for each of the alternatives with an WriterContext
 * that creates encrypted streams.
 */
public class EncryptionTreeWriter implements TreeWriter {
  // the different writers
  final TreeWriter[] childrenWriters;
  final ColumnEncryption[] keys;
  final DataMask[] masks;
  // a column vector that we use to apply the masks
  final VectorizedRowBatch scratch;

  public EncryptionTreeWriter(int columnId,
                              TypeDescription schema,
                              WriterContext context) throws IOException {
    ColumnEncryption columnEncryption = context.getEncryption(columnId);
    scratch = schema.createRowBatch();
    childrenWriters = new TreeWriterBase[2];
    keys = new ColumnEncryption[childrenWriters.length];
    masks = new DataMask[childrenWriters.length];

    // no mask, encrypted data
    masks[0] = null;
    keys[0] = columnEncryption;
    childrenWriters[0] = Factory.create(schema,
        new EncryptionWriterContext(context, columnEncryption), true);

    // masked unencrypted
    masks[1] = columnEncryption.getUnencryptedMask().create(schema);
    keys[1] = null;
    childrenWriters[1] = Factory.create(schema,
        new EncryptionWriterContext(context, null), true);

  }

  @Override
  public void writeRootBatch(VectorizedRowBatch batch, int offset,
                             int length) throws IOException {
    scratch.ensureSize(length);
    for(int alt=0; alt < childrenWriters.length; ++alt) {
      // if there is a mask, apply it to each column
      if (masks[alt] != null) {
        for(int col=0; col < scratch.cols.length; ++col) {
          masks[alt].maskData(batch.cols[col], scratch.cols[col], offset,
              length);
        }
        childrenWriters[alt].writeRootBatch(scratch, offset, length);
      } else {
        childrenWriters[alt].writeRootBatch(batch, offset, length);
      }
    }
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    for(int alt=0; alt < childrenWriters.length; ++alt) {
      // if there is a mask, apply it to each column
      if (masks[alt] != null) {
        masks[alt].maskData(vector, scratch.cols[0], offset, length);
        childrenWriters[alt].writeBatch(scratch.cols[0], offset, length);
      } else {
        childrenWriters[alt].writeBatch(vector, offset, length);
      }
    }
  }

  @Override
  public void createRowIndexEntry() throws IOException {
    for(TreeWriter child: childrenWriters) {
      child.createRowIndexEntry();
    }
  }

  @Override
  public void writeStripe(OrcProto.StripeFooter.Builder builder,
                          OrcProto.StripeStatistics.Builder stats,
                          int requiredIndexEntries) throws IOException {
    OrcProto.StripeEncryption.Builder stripeEncrypt =
        builder.getEncryptionBuilder();
    for(int c = 0; c < childrenWriters.length; ++c) {
      OrcProto.StripeFooter.Builder localFooter =
          keys[c] == null ? builder :
              stripeEncrypt.getFooterBuilder(keys[c].getKey().getId());
      childrenWriters[c].writeStripe(localFooter, stats, requiredIndexEntries);
    }
  }

  @Override
  public void updateFileStatistics(OrcProto.StripeStatistics stats) {
    for(TreeWriter child: childrenWriters) {
      child.updateFileStatistics(stats);
    }
  }

  @Override
  public long estimateMemory() {
    long result = 0;
    for (TreeWriter writer : childrenWriters) {
      result += writer.estimateMemory();
    }
    return result;
  }

  @Override
  public long getRawDataSize() {
    long result = 0;
    for (TreeWriter writer : childrenWriters) {
      result += writer.getRawDataSize();
    }
    return result;
  }

  @Override
  public void writeFileStatistics() {
    for (TreeWriter child : childrenWriters) {
      child.writeFileStatistics();
    }
  }

  static class EncryptionWriterContext implements WriterContext {
    private final WriterContext parent;
    private final ColumnEncryption encryption;

    EncryptionWriterContext(WriterContext parent,
                            ColumnEncryption encryption) {
      this.parent = parent;
      this.encryption = encryption;
    }

    @Override
    public OutStream createStream(int column,
                                  OrcProto.Stream.Kind kind) throws IOException {
      EncryptionKey key = getKey();
      if (key == EncryptionKey.UNENCRYPTED) {
        return parent.createStream(column, kind);
      } else {
        final StreamName name = new StreamName(column, kind, key.getId());
        CompressionCodec codec = getCustomizedCodec(kind);
        HadoopShims.KeyMetadata keyMetadata = key.getKeyVersion();
        return new OutStream(name, getBufferSize(), codec,
            keyMetadata.getAlgorithm(),
            encryption.getMaterial(),
            getReceiver(name));
      }
    }

    @Override
    public int getRowIndexStride() {
      return parent.getRowIndexStride();
    }

    @Override
    public boolean buildIndex() {
      return parent.buildIndex();
    }

    @Override
    public boolean isCompressed() {
      return parent.isCompressed();
    }

    @Override
    public OrcFile.EncodingStrategy getEncodingStrategy() {
      return parent.getEncodingStrategy();
    }

    @Override
    public boolean[] getBloomFilterColumns() {
      return parent.getBloomFilterColumns();
    }

    @Override
    public double getBloomFilterFPP() {
      return parent.getBloomFilterFPP();
    }

    @Override
    public Configuration getConfiguration() {
      return parent.getConfiguration();
    }

    @Override
    public OrcFile.Version getVersion() {
      return parent.getVersion();
    }

    @Override
    public OrcFile.BloomFilterVersion getBloomFilterVersion() {
      return parent.getBloomFilterVersion();
    }

    @Override
    public void writeIndex(StreamName name, OrcProto.RowIndex.Builder index) throws IOException {
      parent.writeIndex(name, index);
    }

    @Override
    public void writeBloomFilter(StreamName name, OrcProto.BloomFilterIndex.Builder bloom) throws IOException {
      parent.writeBloomFilter(name, bloom);
    }

    @Override
    public int getBufferSize() {
      return parent.getBufferSize();
    }

    @Override
    public PhysicalWriter.OutputReceiver getReceiver(StreamName name) throws IOException {
      return parent.getReceiver(name);
    }

    @Override
    public CompressionCodec getCustomizedCodec(OrcProto.Stream.Kind kind) {
      return parent.getCustomizedCodec(kind);
    }

    // We are already encrypted, so there are no more encryption
    @Override
    public ColumnEncryption getEncryption(int columnId) {
      return null;
    }

    @Override
    public EncryptionKey getKey() {
      return encryption == null ?
          EncryptionKey.UNENCRYPTED :
          encryption.getKey();
    }

    @Override
    public void writeFileStatistics(int column, OrcProto.ColumnStatistics.Builder stats) {

    }
  }
}
