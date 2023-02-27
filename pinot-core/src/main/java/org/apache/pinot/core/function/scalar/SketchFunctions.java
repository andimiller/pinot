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
package org.apache.pinot.core.function.scalar;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.math.BigDecimal;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Inbuilt Sketch Transformation Functions
 * The functions can be used as UDFs in Query when added in the FunctionRegistry.
 * @ScalarFunction annotation is used with each method for the registration
 *
 * These are intended to be used during ingestion to create sketches from raw data, which can be rolled up later.
 *
 * Note this is defined in pinot-core rather than pinot-common because pinot-core has dependencies on
 * datasketches/clearspring analytics.
 *
 * Example usage:
 * {
 *   "transformConfigs": [
 *     {
 *       "columnName": "players",
 *       "transformFunction": "DistinctCountRawThetaSketch(playerID)"
 *     },
 *     {
 *       "columnName": "names",
 *       "transformFunction": "DistinctCountRawHLL(playerName)"
 *     }
 *   ]
 * }
 */
public class SketchFunctions {
  private SketchFunctions() {
  }

  /**
   * Create a Theta Sketch containing the input
   *
   * @param input an Object we want to insert into the sketch, may be null to return an empty sketch
   * @return serialized theta sketch as bytes
   */
  @ScalarFunction(nullableParameters = true)
  public static byte[] distinctCountRawThetaSketch(Object input) {
    // TODO make nominal entries configurable
    UpdateSketch sketch =
        Sketches.updateSketchBuilder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES)
            .build();
    if (input instanceof String) {
      sketch.update((String) input);
    } else if (input instanceof Long) {
      sketch.update((Long) input);
    } else if (input instanceof Integer) {
      sketch.update((Integer) input);
    } else if (input instanceof BigDecimal) {
      sketch.update(((BigDecimal) input).toString());
    } else if (input instanceof Float) {
      sketch.update((Float) input);
    } else if (input instanceof Double) {
      sketch.update((Double) input);
    } else if (input instanceof byte[]) {
      sketch.update((byte[]) input);
    }
    return ObjectSerDeUtils.DATA_SKETCH_SER_DE.serialize(sketch.compact());
  }

  /**
   * Create a HyperLogLog containing the input
   *
   * @param input an Object we want to insert into the HLL, may be null to return an empty HLL
   * @return serialized HLL as bytes
   */
  @ScalarFunction(nullableParameters = true)
  public static byte[] distinctCountRawHLL(Object input) {
    // TODO make log2m configurable
    HyperLogLog hll = new HyperLogLog.Builder(CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M).build();
    if (input != null) {
      hll.offer(input);
    }
    return ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hll);
  }
}
