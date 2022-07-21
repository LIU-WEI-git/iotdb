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

package org.apache.iotdb.commons.trigger.service;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.udf.service.SnapshotUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

// TODO extract a util class for Trigger and UDF
public class TriggerExecutableManager implements IService, SnapshotProcessor {
  private final String temporaryLibRoot;
  private final String triggerLibRoot;

  private final AtomicLong requestCounter;

  public TriggerExecutableManager(String temporaryLibRoot, String triggerLibRoot) {
    this.temporaryLibRoot = temporaryLibRoot;
    this.triggerLibRoot = triggerLibRoot;
    requestCounter = new AtomicLong(0);
  }

  public TriggerExecutableResource request(List<String> uris)
      throws URISyntaxException, IOException {
    final long requestId = generateNextRequestId();
    downloadExecutables(uris, requestId);
    return new TriggerExecutableResource(requestId, getDirStringByRequestId(requestId));
  }

  public void moveToExtLibDir(TriggerExecutableResource resource, String triggerName)
      throws IOException {
    FileUtils.moveDirectory(
        getDirByRequestId(resource.getRequestId()), getDirByTriggerName(triggerName));
  }

  public void removeFromTemporaryLibRoot(TriggerExecutableResource resource) {
    removeFromTemporaryLibRoot(resource.getRequestId());
  }

  public void removeFromExtLibDir(String triggerName) {
    FileUtils.deleteQuietly(getDirByTriggerName(triggerName));
  }

  private synchronized long generateNextRequestId() throws IOException {
    long requestId = requestCounter.getAndIncrement();
    while (FileUtils.isDirectory(getDirByRequestId(requestId))) {
      requestId = requestCounter.getAndIncrement();
    }
    FileUtils.forceMkdir(getDirByRequestId(requestId));
    return requestId;
  }

  private void downloadExecutables(List<String> uris, long requestId)
      throws IOException, URISyntaxException {
    // TODO: para download
    try {
      for (String uriString : uris) {
        final URL url = new URI(uriString).toURL();
        final String fileName = uriString.substring(uriString.lastIndexOf("/") + 1);
        final String destination =
            temporaryLibRoot + File.separator + requestId + File.separator + fileName;
        FileUtils.copyURLToFile(url, FSFactoryProducer.getFSFactory().getFile(destination));
      }
    } catch (Exception e) {
      removeFromTemporaryLibRoot(requestId);
      throw e;
    }
  }

  private void removeFromTemporaryLibRoot(long requestId) {
    FileUtils.deleteQuietly(getDirByRequestId(requestId));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // dir string and dir file generation
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public File getDirByRequestId(long requestId) {
    return FSFactoryProducer.getFSFactory().getFile(getDirStringByRequestId(requestId));
  }

  public String getDirStringByRequestId(long requestId) {
    return temporaryLibRoot + File.separator + requestId + File.separator;
  }

  public File getDirByTriggerName(String triggerName) {
    return FSFactoryProducer.getFSFactory().getFile(getDirStringByTriggerName(triggerName));
  }

  public String getDirStringByTriggerName(String triggerName) {
    return triggerLibRoot + File.separator + triggerName + File.separator;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IService
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void start() throws StartupException {
    try {
      makeDirIfNecessary(temporaryLibRoot);
      makeDirIfNecessary(triggerLibRoot);
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  @Override
  public void stop() {
    // nothing to do
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TRIGGER_EXECUTABLE_MANAGER_SERVICE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static TriggerExecutableManager INSTANCE = null;

  public static synchronized TriggerExecutableManager setupAndGetInstance(
      String temporaryLibRoot, String triggerLibRoot) {
    if (INSTANCE == null) {
      INSTANCE = new TriggerExecutableManager(temporaryLibRoot, triggerLibRoot);
    }
    return INSTANCE;
  }

  private static void makeDirIfNecessary(String dir) throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(dir);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  public static TriggerExecutableManager getInstance() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // SnapshotProcessor
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    return SnapshotUtils.takeSnapshotForDir(
            temporaryLibRoot,
            snapshotDir.getAbsolutePath() + File.separator + "ext" + File.separator + "temporary")
        && SnapshotUtils.takeSnapshotForDir(
            triggerLibRoot,
            snapshotDir.getAbsolutePath() + File.separator + "ext" + File.separator + "trigger");
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    SnapshotUtils.loadSnapshotForDir(
        snapshotDir.getAbsolutePath() + File.separator + "ext" + File.separator + "temporary",
        temporaryLibRoot);
    SnapshotUtils.loadSnapshotForDir(
        snapshotDir.getAbsolutePath() + File.separator + "ext" + File.separator + "trigger",
        triggerLibRoot);
  }
}
