/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.aggregator;

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import com.dynatrace.hash4j.hashing.HashStream64;
import com.dynatrace.hash4j.hashing.Hashing;
import java.math.BigDecimal;
import java.util.Optional;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountULLValueAggregator implements ValueAggregator<Object, UltraLogLog> {

  public static Optional<Long> wyHashObject(Object input) {
    if (input != null) {
      HashStream64 hs = Hashing.wyhashFinal4().hashStream();
      if (input instanceof Integer) {
        return Optional.of(hs.putInt((Integer) input).getAsLong());
      } else if (input instanceof Long) {
        return Optional.of(hs.putLong((Long) input).getAsLong());
      } else if (input instanceof Float) {
        return Optional.of(hs.putFloat((Float) input).getAsLong());
      } else if (input instanceof Double) {
        return Optional.of(hs.putDouble((Double) input).getAsLong());
      } else if (input instanceof BigDecimal) {
        return Optional.of(hs.putString(((BigDecimal) input).toString()).getAsLong());
      } else if (input instanceof String) {
        return Optional.of(hs.putString((String) input).getAsLong());
      } else if (input instanceof byte[]) {
        return Optional.of(hs.putBytes((byte[]) input).getAsLong());
      } else {
        throw new IllegalArgumentException("Unrecognised input type for ULL: " + input.getClass().getName());
      }
    } else {
      return Optional.empty();
    }
  };

  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;
  // p of 8 means 256 bytes of data and 1 byte header
  private static final int DEFAULT_LOG2M_BYTE_SIZE = 257;
  // Byte size won't change once we get the initial aggregated value
  private int _maxByteSize;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.DISTINCTCOUNTULL;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public UltraLogLog getInitialAggregatedValue(Object rawValue) {
    UltraLogLog initialValue;
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
      _maxByteSize = Math.max(_maxByteSize, bytes.length);
    } else {
      // TODO: Handle configurable log2m for StarTreeBuilder
      initialValue = UltraLogLog.create(CommonConstants.Helix.DEFAULT_ULTRALOGLOG_LOG2M);
      wyHashObject(rawValue).ifPresent(initialValue::add);
      _maxByteSize = Math.max(_maxByteSize, DEFAULT_LOG2M_BYTE_SIZE);
    }
    return initialValue;
  }

  @Override
  public UltraLogLog applyRawValue(UltraLogLog value, Object rawValue) {
    if (rawValue instanceof byte[]) {
      try {
        value.add(deserializeAggregatedValue((byte[]) rawValue));
      } catch (IllegalArgumentException e) {
        throw new RuntimeException(e);
      }
    } else {
      wyHashObject(rawValue).ifPresent(value::add);
    }
    return value;
  }

  @Override
  public UltraLogLog applyAggregatedValue(UltraLogLog value, UltraLogLog aggregatedValue) {
    try {
      value.add(aggregatedValue);
      return value;
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public UltraLogLog cloneAggregatedValue(UltraLogLog value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(UltraLogLog value) {
    return CustomSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.serialize(value);
  }

  @Override
  public UltraLogLog deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize(bytes);
  }
}
