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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.trigger.service.TriggerExecutableManager;
import org.apache.iotdb.commons.trigger.service.TriggerExecutableResource;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.CreateTriggerPlan;
import org.apache.iotdb.db.engine.trigger.service.TriggerClassLoader;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TriggerInfo implements SnapshotProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerInfo.class);

  private static final ConfigNodeConfig CONFIG_NODE_CONF =
      ConfigNodeDescriptor.getInstance().getConf();

  private final TriggerExecutableManager triggerExecutableManager;
  private final TriggerRegistrationService triggerRegistrationService;

  public TriggerInfo() {
    triggerExecutableManager =
        TriggerExecutableManager.setupAndGetInstance(
            CONFIG_NODE_CONF.getTemporaryLibDir(), CONFIG_NODE_CONF.getTriggerLibDir());
    triggerRegistrationService = TriggerRegistrationService.getInstance();
  }

  public synchronized void validateBeforeRegistration(
      String triggerName,
      byte event,
      String fullPath,
      String className,
      Map<String, String> attributes,
      List<String> uris)
      throws Exception {
    triggerRegistrationService.validate(triggerName, event, fullPath, className, attributes);

    if (uris.isEmpty()) {
      fetchExecutablesAndCheckInstantiation(className);
    } else {
      fetchExecutablesAndCheckInstantiation(className, uris);
    }
  }

  private void fetchExecutablesAndCheckInstantiation(String className) throws Exception {
    try (TriggerClassLoader tempTriggerClassLoader =
        new TriggerClassLoader(CONFIG_NODE_CONF.getTriggerLibDir())) {
      Class.forName(className, true, tempTriggerClassLoader).getDeclaredConstructor().newInstance();
    }
  }

  private void fetchExecutablesAndCheckInstantiation(String className, List<String> uris)
      throws Exception {
    final TriggerExecutableResource resource = triggerExecutableManager.request(uris);
    try (TriggerClassLoader tempTriggerClassLoader =
        new TriggerClassLoader(resource.getResourceDir())) {
      Class.forName(className, true, tempTriggerClassLoader).getDeclaredConstructor().newInstance();
    } finally {
      triggerExecutableManager.removeFromTemporaryLibRoot(resource);
    }
  }

  public synchronized TSStatus createTrigger(CreateTriggerPlan plan) {
    final String triggerName = plan.getTriggerName();
    final byte event = plan.getEvent();
    final String fullPath = plan.getFullPath();
    final String className = plan.getClassName();
    final Map<String, String> attributes = plan.getAttributes();
    final List<String> uris = plan.getUris();

    try {
      triggerRegistrationService.register(
          triggerName,
          event,
          fullPath,
          className,
          attributes,
          uris,
          TriggerExecutableManager.getInstance());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "[ConfigNode] Failed to register Trigger %s(class name: %s, uris: %s), because of exception: %s",
              triggerName, className, uris, e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    return triggerExecutableManager.processTakeSnapshot(snapshotDir)
        && triggerRegistrationService.processTakeSnapshot(snapshotDir);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {}
}
