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
package org.apache.pinot.core.operator.transform.function;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Inbuilt Transformation Functions
 * The functions can be used as UDFs in Query when added in the FunctionRegistry.
 * @ScalarFunction annotation is used with each method for the registration
 *
 * Example usage:
 * <code> SELECT SHA(bytesColumn) FROM baseballStats LIMIT 10 </code>
 * <code> SELECT SHA256(bytesColumn) FROM baseballStats LIMIT 10 </code>
 * <code> SELECT SHA512(bytesColumn) FROM baseballStats LIMIT 10 </code>
 * <code> SELECT MD5(bytesColumn) FROM baseballStats LIMIT 10 </code>
 */
public class SketchFunctions {
  private SketchFunctions() {
  }

  /**
   * Return SHA-1 digest as hex string.
   *
   * @param input the byte array representing the data
   * @return hash string in hex format
   */
  @ScalarFunction
  public static byte[] theta(byte[] input) {
    UpdateSketch sketch = Sketches.updateSketchBuilder().build();
    sketch.update(input);
    return ObjectSerDeUtils.DATA_SKETCH_SER_DE.serialize(sketch.compact());
  }

  /**
   * Return SHA-256 digest as hex string.
   *
   * @param input the byte array representing the data
   * @return hash string in hex format
   */
  @ScalarFunction
  public static byte[] HLL(byte[] input) {
    HyperLogLog hll = new HyperLogLog.Builder(CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M).build();
    hll.offer(input);
    return ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hll);
  }


}
