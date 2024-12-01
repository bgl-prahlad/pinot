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
package org.apache.pinot.tools.admin.command;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.PinotSegmentURIChanger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class to implement SyncSegmentDeepStoreLocation command.
 * Pre-condition:
 */
@CommandLine.Command(name = "SyncSegmentDeepStore")
public class SyncSegmentDeepStoreLocationCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(SyncSegmentDeepStoreLocationCommand.class);

  @CommandLine.Option(names = {"-zkAddress"}, required = false, description = "HTTP address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @CommandLine.Option(names = {"-clusterName"}, required = false, description = "Pinot cluster clusterName.")
  private String _clusterName = DEFAULT_CLUSTER_NAME;

  @CommandLine.Option(names = {"-numThreads"}, description = "Parallelism while migrating segments, default is 1.")
  private int _numThreads = 1;

  @CommandLine.Option(names = {"-overwrite"}, description = "Overwrite existing segment files, default is false.")
  private boolean _overWrite = false;

  @SuppressWarnings("FieldCanBeLocal")
  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, help = true, description = "Print this message.")
  private boolean _help = false;

  private ZKHelixAdmin _helixAdmin;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private String _controllerAddress;

  SyncSegmentDeepStoreLocationCommand() {
    super();
  }

  public SyncSegmentDeepStoreLocationCommand setOverwrite(boolean overwrite) {
    _overwrite = overwrite;
    return this;
  }

  public SyncSegmentDeepStoreLocationCommand setNumThreads(int numThreads) {
    _numThreads = numThreads;
    return this;
  }

  @Override
  public String toString() {
    return String.format(
        "SyncSegmentDeepStore -overwrite %s -numThreads %d",
        _overwrite, _numThreads);
  }

  @Override
  public final String getName() {
    return "SyncSegmentDeepStore";
  }

  @Override
  public String description() {
    return "Copy segments if necessary to new data-location and update ZK metadata";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  private boolean init() {
    LOGGER.info("Trying to connect to {} cluster {}", _zkAddress, _clusterName);
    _helixAdmin = new ZKHelixAdmin(_zkAddress);

    if (!_helixAdmin.getClusters().contains(_clusterName)) {
      LOGGER.error("Cluster {} not found in {}.", _clusterName, _zkAddress);
      return false;
    }
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    String path = PropertyPathBuilder.propertyStore(_clusterName);
    _propertyStore = new ZkHelixPropertyStore<>(_zkAddress, serializer, path);

    return true;
  }

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Executing command: {}", toString());

    if ( !init() ) { return false; }

    List<String> tables = _helixAdmin.getResourcesInCluster(_clusterName);

    // (optional) Check that the Helix cluster is in maintenance mode
    // Get access to the lead controller
    // Get the current deep store location - configuration of the lead controller

    for (String table : tables) {
      // Skip non-table resources
      if (!TableNameBuilder.isTableResource(table)) {
        continue;
      }

      // It is sufficient to pick up the segments from the ideal state of the table
      // the use-case that this command is initially targeting assumes a manual batch
      // workflow. It does not consider scenarios where the segments being modified
      // in a live deployment - eg. REFRESH operation being active etc.
      // The general problem probably needs to be modelled using Helix state-machine to
      // ensure that the segment update is safe to be performed in the presence of
      // other segment mutating operations.
      IdealState idealState = _helixAdmin.getResourceIdealState(_clusterName, table);
      Set<String> segmentsFromIdealState = idealState.getPartitionSet();

      for (String segment : segmentsFromIdealState) {
        SegmentZKMetadata segmentZKMetadata = ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, table, segment);

        // continue if URI of segment is contained in the controller deep-store
        // Check that segment URI is a local file-system URI - Log an error if this is not the case, skip this segment

        // submit executor service job to copy file from one FS to another using PinotFS
      }
    }
/*
    Future[] futures;
    try (ExecutorService executorService = Executors.newFixedThreadPool(_numThreads)) {
      numDataFiles = dataFiles.size();
      futures = new Future[numDataFiles];
      for (int i = 0; i < numDataFiles; i++) {
        int sequenceId = i;
        futures[sequenceId] = executorService.submit(() -> {return null;});
      }
      executorService.shutdown();
    }
    for (Future future : futures) {
      future.get();
    }
    LOGGER.info("Successfully migrated {} segments to : {}", numDataFiles, dataFiles);
  */
    return true;
  }

  static class SegmentInfo {
    @JsonProperty("name")
    public String _name;
    @JsonProperty("segmentStateMap")
    public Map<String, String> _segmentStateMap = new HashMap<String, String>();
  }

  static class TableInfo {
    @JsonProperty("tableName")
    public String _tableName;
    @JsonProperty("tag")
    public String _tag;
    @JsonProperty("segmentInfoList")
    public List<ShowClusterInfoCommand.SegmentInfo> _segmentInfoList = new ArrayList<ShowClusterInfoCommand.SegmentInfo>();

    public void addSegmentInfo(ShowClusterInfoCommand.SegmentInfo segmentInfo) {
      _segmentInfoList.add(segmentInfo);
    }
  }

}
