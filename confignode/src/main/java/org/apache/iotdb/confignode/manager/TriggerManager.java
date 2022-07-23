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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.write.CreateTriggerPlan;
import org.apache.iotdb.confignode.persistence.TriggerInfo;
import org.apache.iotdb.mpp.rpc.thrift.TCreateTriggerRequest;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TriggerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerManager.class);

  private final ConfigManager configManager;
  private final TriggerInfo triggerInfo;

  public TriggerManager(ConfigManager configManager, TriggerInfo triggerInfo) {
    this.configManager = configManager;
    this.triggerInfo = triggerInfo;
  }

  public TSStatus createTrigger(
      String triggerName,
      byte event,
      String fullPath,
      String className,
      Map<String, String> attributes,
      List<String> uris) {
    try {
      triggerInfo.validateBeforeRegistration(
          triggerName, event, fullPath, className, attributes, uris);

      final TSStatus configNodeStatus =
          configManager
              .getConsensusManager()
              .write(
                  new CreateTriggerPlan(triggerName, event, fullPath, className, attributes, uris))
              .getStatus();
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != configNodeStatus.getCode()) {
        return configNodeStatus;
      }
      return RpcUtils.squashResponseStatusList(
          createTriggerOnDataNode(triggerName, event, fullPath, className, attributes, uris));
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to register Trigger %s(class name: %s, uris: %s), because of exception: %s",
              triggerName, className, uris, e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  private List<TSStatus> createTriggerOnDataNode(
      String triggerName,
      byte event,
      String fullPath,
      String className,
      Map<String, String> attributes,
      List<String> uris) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations(-1);
    final List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(dataNodeLocationMap.size()));
    final TCreateTriggerRequest request =
        new TCreateTriggerRequest(triggerName, event, fullPath, className, attributes, uris);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            request,
            dataNodeLocationMap,
            DataNodeRequestType.CREATE_TRIGGER,
            dataNodeResponseStatus);
    return dataNodeResponseStatus;
  }
}
