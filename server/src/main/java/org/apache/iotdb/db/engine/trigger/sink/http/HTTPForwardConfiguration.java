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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.trigger.sink.http;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.trigger.sink.api.Configuration;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;

public class HTTPForwardConfiguration implements Configuration {
  private final boolean stopIfException;

  // HTTP config items
  private final String endpoint;
  private final int poolSize;
  private final int poolMaxPerRoute;

  private final String device;
  private final String measurement;

  public HTTPForwardConfiguration(
      String endpoint,
      boolean stopIfException,
      int poolSize,
      int poolMaxPerRoute,
      String device,
      String measurement) {
    this.endpoint = endpoint;
    this.stopIfException = stopIfException;
    this.poolSize = poolSize;
    this.poolMaxPerRoute = poolMaxPerRoute;
    this.device = device;
    this.measurement = measurement;
  }

  public void checkConfig() throws SinkException {
    if (endpoint == null || endpoint.isEmpty()) {
      throw new SinkException("HTTP config item error");
    } else if (device != null || measurement != null) {
      try {
        new PartialPath(device, measurement);
      } catch (IllegalPathException e) {
        throw new SinkException("HTTP config item error", e);
      }
    }
  }

  public boolean isStopIfException() {
    return stopIfException;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public int getPoolSize() {
    return poolSize;
  }

  public int getPoolMaxPerRoute() {
    return poolMaxPerRoute;
  }

  public String getDevice() {
    return device;
  }

  public String getMeasurement() {
    return measurement;
  }
}
