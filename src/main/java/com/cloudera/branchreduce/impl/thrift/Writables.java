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
package com.cloudera.branchreduce.impl.thrift;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Lists;

public class Writables {

  private static final Log LOG = LogFactory.getLog(Writables.class);
  
  private static final Configuration DUMMY = new Configuration();
  
  public static ByteBuffer toByteBuffer(Writable writable) {
    return ByteBuffer.wrap(WritableUtils.toByteArray(writable));
  }
  
  public static <T extends Writable> T fromByteBuffer(ByteBuffer bb, Class<T> clazz) {
    T instance = ReflectionUtils.newInstance(clazz, DUMMY);
    try {
      instance.readFields(new DataInputStream(new ByteArrayInputStream(bb.array(),
          bb.arrayOffset(), bb.limit())));
    } catch (IOException e) {
      LOG.error("Deserialization error for class: " + clazz, e);
    }
    return instance;
  }
  
  public static <T extends Writable> List<T> fromByteBuffer(
      List<ByteBuffer> bbs, Class<T> clazz) {
    List<T> values = Lists.newArrayList();
    for (ByteBuffer bb : bbs) {
      values.add(fromByteBuffer(bb, clazz));
    }
    return values;
  }
}
