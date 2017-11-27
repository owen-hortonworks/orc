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

import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.security.Key;

public class ColumnEncryption {
  private final EncryptionKey key;
  private final int root;
  private final MaskDescription unencryptedMask;
  private final Key material;

  public ColumnEncryption(EncryptionKey key,
                          int columnId,
                          MaskDescription mask,
                          Key columnKey) throws IOException {
    this.key = key;
    this.root = columnId;
    this.unencryptedMask = mask;
    this.material = columnKey;
  }

  public EncryptionKey getKey() {
    return key;
  }

  public int getRoot() {
    return root;
  }

  public MaskDescription getUnencryptedMask() {
    return unencryptedMask;
  }

  public Key getMaterial() {
    return material;
  }
}

