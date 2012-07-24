/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.branchreduce;

import org.apache.hadoop.io.Writable;

/**
 * An extension of {@code Writable} that supports a <i>merge</i> operation, which is
 * a deterministic rule for combining the value of this instance with the value of another
 * instance.
 */
public interface GlobalState<GS extends GlobalState<GS>> extends Writable {
  /**
   * Merge the state of this object with the given object, returning true
   * if the merge resulted in any modifications to this object.
   */
  boolean mergeWith(GS other);
}
