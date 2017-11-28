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
import org.apache.orc.CompressionCodec;
import org.apache.orc.DataMask;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.StreamName;

import java.io.IOException;

public interface WriterContext {

  /**
   * Create a stream to store part of a column.
   * @param column the column id for the stream
   * @param kind the kind of stream
   * @return The output outStream that the section needs to be written to.
   */
  OutStream createStream(int column,
                         OrcProto.Stream.Kind kind
  ) throws IOException;

  /**
   * Get the stride rate of the row index.
   */
  int getRowIndexStride();

  /**
   * Should be building the row index.
   * @return true if we are building the index
   */
  boolean buildIndex();

  /**
   * Is the ORC file compressed?
   * @return are the streams compressed
   */
  boolean isCompressed();

  /**
   * Get the encoding strategy to use.
   * @return encoding strategy
   */
  OrcFile.EncodingStrategy getEncodingStrategy();

  /**
   * Get the bloom filter columns
   * @return bloom filter columns
   */
  boolean[] getBloomFilterColumns();

  /**
   * Get bloom filter false positive percentage.
   * @return fpp
   */
  double getBloomFilterFPP();

  /**
   * Get the writer's configuration.
   * @return configuration
   */
  Configuration getConfiguration();

  /**
   * Get the version of the file to write.
   */
  OrcFile.Version getVersion();

  OrcFile.BloomFilterVersion getBloomFilterVersion();

  void writeIndex(StreamName name,
                  OrcProto.RowIndex.Builder index) throws IOException;

  void writeBloomFilter(StreamName name,
                        OrcProto.BloomFilterIndex.Builder bloom
                        ) throws IOException;

  int getBufferSize();

  PhysicalWriter.OutputReceiver getReceiver(StreamName name) throws IOException;

  /**
   * Get a compression codec for the given stream kind.
   * @param kind the kind of the stream
   * @return a codec for compression
   */
  CompressionCodec getCustomizedCodec(OrcProto.Stream.Kind kind);

  /**
   * Get the encryption rooted at the given column.
   * @param columnId the given column
   */
  ColumnEncryption getEncryption(int columnId);

  /**
   * Create a data mask
   */
  DataMask createMask(int maskId, TypeDescription schema);

  /**
   * Get the encryption key that this subtree is using.
   * @return the encryption key
   */
  EncryptionKey getKey();

  /**
   * Get the PhysicalWriter.
   * @return the file's physical writer.
   */
  PhysicalWriter getPhysicalWriter();
}
