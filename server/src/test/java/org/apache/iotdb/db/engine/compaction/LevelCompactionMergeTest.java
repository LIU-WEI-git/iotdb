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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.TsFileManagement.CompactionMergeTask;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LevelCompactionMergeTest extends LevelCompactionTest {

  File tempSGDir;
  boolean compactionMergeWorking = false;

  @Override
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    tempSGDir = new File(TestConstant.getTestTsFileDir("root.compactionTest", 0, 0));
    if (!tempSGDir.exists()) {
      Assert.assertTrue(tempSGDir.mkdirs());
    }
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  /** just compaction once */
  @Test
  public void testCompactionMergeOnce() throws IllegalPathException, IOException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    levelCompactionTsFileManagement.forkCurrentFileList(0);
    CompactionMergeTask compactionMergeTask =
        levelCompactionTsFileManagement
        .new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeWorking = true;
    compactionMergeTask.call();
    while (compactionMergeWorking) {
      // wait
    }

    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            levelCompactionTsFileManagement.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
      }
    }
  }

  /** just compaction stable list */
  @Test
  public void testCompactionMergeStableList() throws IllegalPathException, IOException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(2);
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    levelCompactionTsFileManagement.forkCurrentFileList(0);
    CompactionMergeTask compactionMergeTask =
        levelCompactionTsFileManagement
        .new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeWorking = true;
    compactionMergeTask.call();
    while (compactionMergeWorking) {
      // wait
    }

    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            levelCompactionTsFileManagement.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        count++;
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
      }
    }
    assertEquals(500, count);
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }

  /**
   * As we change the structure of mods file in 0.12, we have to check whether a modification record
   * is valid by its offset in tsfile
   */
  @Test
  public void testCompactionModsByOffsetAfterMerge() throws IllegalPathException, IOException {
    int prevPageLimit =
        IoTDBDescriptor.getInstance().getConfig().getMergePagePointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergePagePointNumberThreshold(1);

    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    TsFileResource forthSeqTsFileResource = seqResources.get(3);
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    try (ModificationFile sourceModificationFile =
        new ModificationFile(
            forthSeqTsFileResource.getTsFilePath() + ModificationFile.FILE_SUFFIX)) {
      Modification modification =
          new Deletion(path, forthSeqTsFileResource.getTsFileSize() / 10, 300, 310);
      sourceModificationFile.write(modification);
    }
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    levelCompactionTsFileManagement.forkCurrentFileList(0);
    CompactionMergeTask compactionMergeTask =
        levelCompactionTsFileManagement
        .new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeWorking = true;
    compactionMergeTask.call();
    while (compactionMergeWorking) {
      // wait
    }

    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            levelCompactionTsFileManagement.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);

    long count = 0L;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      count += batchData.length();
    }
    assertEquals(489, count);

    List<TsFileResource> tsFileResourceList = levelCompactionTsFileManagement.getTsFileList(true);
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.getModFile().remove();
      tsFileResource.remove();
    }
    IoTDBDescriptor.getInstance().getConfig().setMergePagePointNumberThreshold(prevPageLimit);
  }

  /** test append chunk merge, the chunk is already large than merge_chunk_point_number */
  @Test
  public void testCompactionAppendChunkMerge() throws IOException {
    int prevMergeChunkPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(1);

    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    levelCompactionTsFileManagement.forkCurrentFileList(0);
    CompactionMergeTask compactionMergeTask =
        levelCompactionTsFileManagement
        .new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeWorking = true;
    compactionMergeTask.call();
    while (compactionMergeWorking) {
      // wait
    }
    TsFileResource newTsFileResource =
        levelCompactionTsFileManagement.getTsFileListByTimePartition(true, 0).get(0);
    TsFileSequenceReader tsFileSequenceReader =
        new TsFileSequenceReader(newTsFileResource.getTsFilePath());
    Map<String, List<ChunkMetadata>> sensorChunkMetadataListMap =
        tsFileSequenceReader.readChunkMetadataInDevice(deviceIds[0]);
    for (List<ChunkMetadata> chunkMetadataList : sensorChunkMetadataListMap.values()) {
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        assertEquals(20, chunkMetadata.getNumOfPoints());
      }
    }
    tsFileSequenceReader.close();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergeChunkPointNumberThreshold(prevMergeChunkPointNumberThreshold);
  }

  /** test not append chunk merge, the chunk is smaller than merge_chunk_point_number */
  @Test
  public void testCompactionNoAppendChunkMerge() throws IOException {
    int prevMergeChunkPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(100000);

    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    levelCompactionTsFileManagement.forkCurrentFileList(0);
    CompactionMergeTask compactionMergeTask =
        levelCompactionTsFileManagement
        .new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeWorking = true;
    compactionMergeTask.call();
    while (compactionMergeWorking) {
      // wait
    }
    TsFileResource newTsFileResource =
        levelCompactionTsFileManagement.getTsFileListByTimePartition(true, 0).get(0);
    TsFileSequenceReader tsFileSequenceReader =
        new TsFileSequenceReader(newTsFileResource.getTsFilePath());
    Map<String, List<ChunkMetadata>> sensorChunkMetadataListMap =
        tsFileSequenceReader.readChunkMetadataInDevice(deviceIds[0]);
    for (List<ChunkMetadata> chunkMetadataList : sensorChunkMetadataListMap.values()) {
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        assertEquals(500, chunkMetadata.getNumOfPoints());
      }
    }
    tsFileSequenceReader.close();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergeChunkPointNumberThreshold(prevMergeChunkPointNumberThreshold);
  }

  /** close compaction merge callback, to release some locks */
  private void closeCompactionMergeCallBack(
      boolean isMergeExecutedInCurrentTask, long timePartitionId) {
    this.compactionMergeWorking = false;
  }

  @Test
  public void testCompactionDiffTimeSeries()
      throws IOException, WriteProcessException, IllegalPathException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(2);
    List<TsFileResource> compactionFiles = prepareTsFileResources();
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(compactionFiles, true);

    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[1].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[1].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            levelCompactionTsFileManagement.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        count++;
      }
    }
    assertEquals(count, 1);

    levelCompactionTsFileManagement.forkCurrentFileList(0);
    CompactionMergeTask compactionOnePartitionUtil =
        levelCompactionTsFileManagement
        .new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeWorking = true;
    compactionOnePartitionUtil.call();
    while (compactionMergeWorking) {
      // wait
    }

    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[1].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            levelCompactionTsFileManagement.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        count++;
      }
    }
    assertEquals(count, 1);
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }
}
