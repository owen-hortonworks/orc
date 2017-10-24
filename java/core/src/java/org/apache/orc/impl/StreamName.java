/**
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

/**
 * The name of a stream within a stripe.
 */
public class StreamName implements Comparable<StreamName> {
  public static final int UNENCRYPTED = 0;
  private final int column;
  private final OrcProto.Stream.Kind kind;
  private final int key;

  public enum Area {
    DATA, INDEX, ENCRYPTED_DATA, ENCRYPTED_INDEX, FOOTER
  }

  public StreamName(int column, OrcProto.Stream.Kind kind) {
    this(column, kind, UNENCRYPTED);
  }

  public StreamName(int column, OrcProto.Stream.Kind kind, int key) {
    this.column = column;
    this.kind = kind;
    this.key = key;
  }

  public boolean equals(Object obj) {
    if (obj != null && obj instanceof  StreamName) {
      StreamName other = (StreamName) obj;
      return other.key == key && other.column == column && other.kind == kind;
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(StreamName streamName) {
    if (streamName == null) {
      return -1;
    }
    if (key != streamName.key) {
      return key < streamName.key ? -1 : 1;
    }
    Area area = getArea();
    Area otherArea = streamName.getArea();
    if (area != otherArea) {
      return otherArea.compareTo(area);
    } else if (column != streamName.column) {
      return column < streamName.column ? -1 : 1;
    }
    return kind.compareTo(streamName.kind);
  }

  public int getColumn() {
    return column;
  }

  public OrcProto.Stream.Kind getKind() {
    return kind;
  }

  public Area getArea() {
    return getArea(kind, key);
  }

  public static Area getArea(OrcProto.Stream.Kind kind) {
    return getArea(kind, UNENCRYPTED);
  }

  public static Area getArea(OrcProto.Stream.Kind kind, int encrypted) {
    switch (kind) {
      case FILE_STATISTICS:
      case STRIPE_STATISTICS:
        return Area.FOOTER;
      case ROW_INDEX:
      case DICTIONARY_COUNT:
      case BLOOM_FILTER:
      case BLOOM_FILTER_UTF8:
        return encrypted == UNENCRYPTED ? Area.INDEX : Area.ENCRYPTED_INDEX;
      default:
        return encrypted == UNENCRYPTED ? Area.DATA : Area.ENCRYPTED_DATA;
    }
  }

  @Override
  public String toString() {
    if (key == UNENCRYPTED) {
      return "Stream for column " + column + " kind " + kind;
    } else {
      return "Stream for column " + column + " kind " + kind + " encryption key: "
          + key;
    }
  }

  @Override
  public int hashCode() {
    return column * 101 + kind.getNumber() + key * 104729;
  }

  public int getKey() {
    return key;
  }

  public boolean isEncrypted() {
    return key != UNENCRYPTED;
  }
}

