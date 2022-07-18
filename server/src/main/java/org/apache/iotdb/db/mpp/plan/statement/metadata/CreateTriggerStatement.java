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

package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.trigger.executor.TriggerEvent;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateTriggerStatement extends Statement implements IConfigStatement {
  private final String triggerName;
  private final TriggerEvent event;
  private final PartialPath fullPath;
  private final String className;
  private final Map<String, String> attributes;
  private final List<URI> uris;

  public CreateTriggerStatement(
      String triggerName,
      TriggerEvent event,
      PartialPath fullPath,
      String className,
      Map<String, String> attributes,
      List<URI> uris) {
    super();
    this.triggerName = triggerName;
    this.event = event;
    this.fullPath = fullPath;
    this.className = className;
    this.attributes = attributes;
    this.uris = uris;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTrigger(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    List<PartialPath> paths = new ArrayList<>(1);
    paths.add(fullPath);
    return paths;
  }

  public String getTriggerName() {
    return triggerName;
  }

  public TriggerEvent getEvent() {
    return event;
  }

  public PartialPath getFullPath() {
    return fullPath;
  }

  public String getClassName() {
    return className;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public List<URI> getUris() {
    return uris;
  }
}
