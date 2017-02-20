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

import org.apache.orc.OrcProto;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestCryptoUtils {

  @Test
  public void testFileId() throws Exception {
    byte[] file1 = CryptoUtils.createFileId();
    byte[] file2 = CryptoUtils.createFileId();
    assertEquals(13, file1.length);
    assertEquals(file1.length, file2.length);
    assertNotEquals(Arrays.asList(file1), Arrays.asList(file2));
  }

  @Test
  public void testCreatePasswordIv() throws Exception {
    byte[] fileId = new byte[13];
    for(int i=0; i < fileId.length; ++i) {
      fileId[i] = (byte) (0x11 * (i + 1));
    }
    byte[] iv = CryptoUtils.createIvForPassword(fileId, 0x123456);
    assertEquals(16, iv.length);
    assertEquals(0x12, iv[0]);
    assertEquals(0x34, iv[1]);
    assertEquals(0x56, iv[2]);
    for(int i=0; i < fileId.length; ++i) {
      assertEquals("i = " + i, fileId[i], iv[3+i]);
    }
  }

  @Test
  public void testCreateStreamIv() throws Exception {
    byte[] iv = CryptoUtils.createIvForStream(new StreamName(0x234567,
        OrcProto.Stream.Kind.BLOOM_FILTER_UTF8), 0x123456);
    assertEquals(16, iv.length);
    assertEquals(0x23, iv[0]);
    assertEquals(0x45, iv[1]);
    assertEquals(0x67, iv[2]);
    assertEquals(0x0, iv[3]);
    assertEquals(0x8, iv[4]);
    assertEquals(0x12, iv[5]);
    assertEquals(0x34, iv[6]);
    assertEquals(0x56, iv[7]);
  }
}
