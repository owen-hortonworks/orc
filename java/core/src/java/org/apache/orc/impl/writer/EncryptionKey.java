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

import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.StreamName;

import java.io.IOException;
import java.security.Key;
import java.util.ArrayList;
import java.util.List;

public class EncryptionKey {
  private final HadoopShims.KeyMetadata metadata;
  private final List<ColumnEncryption> roots = new ArrayList<>();
  private final int id;
  private final byte[] keyIv;
  private Key fileStatsKey;

  public EncryptionKey(HadoopShims.KeyProvider provider,
                       String keyName,
                       int id) throws IOException {
    this.id = id;
    if (provider != null) {
      this.metadata = provider.getCurrentKeyVersion(keyName);
      this.fileStatsKey = provider.getLocalKey(metadata,
          CryptoUtils.createKeyIv(metadata.getAlgorithm()));
      keyIv = CryptoUtils.createKeyIv(metadata.getAlgorithm());
    } else {
      this.metadata = null;
      this.fileStatsKey = null;
      this.keyIv = null;
    }
  }

  public void addRoot(ColumnEncryption root) {
    roots.add(root);
  }

  public HadoopShims.KeyMetadata getMetadata() {
    return metadata;
  }

  public List<ColumnEncryption> getRoots() {
    return roots;
  }

  public int getId() {
    return id;
  }

  public byte[] getKeyIv() {
    return keyIv;
  }

  public Key getFileStatsKey() {
    return fileStatsKey;
  }

  public static final EncryptionKey UNENCRYPTED;

  static {
    try {
      UNENCRYPTED = new EncryptionKey(null, null, StreamName.UNENCRYPTED);
    } catch (IOException e) {
      throw new IllegalStateException("Problem creating unencrypted key", e);
    }
  }
}
