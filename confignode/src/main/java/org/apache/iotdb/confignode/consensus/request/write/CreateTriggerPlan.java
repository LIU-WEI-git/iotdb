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

package org.apache.iotdb.confignode.consensus.request.write;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class CreateTriggerPlan extends ConfigPhysicalPlan {
  private String triggerName;
  private byte event;
  private String fullPath;
  private String className;
  private Map<String, String> attributes;
  private List<String> uris;

  /**
   * This field is mainly used for the stage of recovering trigger registration information, so it
   * will never be serialized into a log file.
   *
   * <p>Note that the status of triggers registered by executing SQL statements is STARTED by
   * default, so this field should be {@code false} by default.
   *
   * @see TriggerRegistrationService
   */
  private boolean isStopped = false;

  public CreateTriggerPlan() {
    super(ConfigPhysicalPlanType.CreateTrigger);
  }

  public CreateTriggerPlan(
      String triggerName,
      byte event,
      String fullPath,
      String className,
      Map<String, String> attributes,
      List<String> uris) {
    super(ConfigPhysicalPlanType.CreateTrigger);
    this.triggerName = triggerName;
    this.event = event;
    this.fullPath = fullPath;
    this.className = className;
    this.attributes = attributes;
    this.uris = uris;
  }

  public String getTriggerName() {
    return triggerName;
  }

  public byte getEvent() {
    return event;
  }

  public String getFullPath() {
    return fullPath;
  }

  public String getClassName() {
    return className;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public List<String> getUris() {
    return uris;
  }

  public boolean isStopped() {
    return isStopped;
  }

  public void markAsStarted() {
    isStopped = false;
  }

  public void markAsStopped() {
    isStopped = true;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    // TODO asymmetric with deserialize
    stream.writeInt(getType().ordinal());

    ReadWriteIOUtils.write(triggerName, stream);
    ReadWriteIOUtils.write(event, stream);
    ReadWriteIOUtils.write(fullPath, stream);
    ReadWriteIOUtils.write(className, stream);
    ReadWriteIOUtils.write(attributes, stream);
    ReadWriteIOUtils.writeStringList(uris, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    triggerName = ReadWriteIOUtils.readString(buffer);
    event = ReadWriteIOUtils.readByte(buffer);
    fullPath = ReadWriteIOUtils.readString(buffer);
    className = ReadWriteIOUtils.readString(buffer);
    attributes = ReadWriteIOUtils.readMap(buffer);
    uris = ReadWriteIOUtils.readStringList(buffer);
  }
}
