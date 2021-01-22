/*
 * Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.bigdata.tidb;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.Objects;

public final class SplitInternal implements Serializable {

  private final TableHandleInternal table;
  private final String startKey;
  private final String endKey;

  public SplitInternal(
      TableHandleInternal table,
      String startKey,
      String endKey) {
    this.table = requireNonNull(table, "table is null");
    this.startKey = requireNonNull(startKey, "startKey is null");
    this.endKey = requireNonNull(endKey, "endKey is null");
  }

  public SplitInternal(
      TableHandleInternal table,
      Base64KeyRange range) {
    this(table, range.getStartKey(), range.getEndKey());
  }

  public TableHandleInternal getTable() {
    return table;
  }

  public String getStartKey() {
    return startKey;
  }

  public String getEndKey() {
    return endKey;
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, startKey, endKey);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    SplitInternal other = (SplitInternal) obj;
    return Objects.equals(this.table, other.table)
        && Objects.equals(this.startKey, other.startKey)
        && Objects.equals(this.endKey, other.endKey);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("startKey", startKey)
        .add("endKey", endKey)
        .toString();
  }
}
