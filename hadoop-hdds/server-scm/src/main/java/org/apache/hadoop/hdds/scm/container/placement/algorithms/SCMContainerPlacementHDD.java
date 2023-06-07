/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.placement.algorithms;


import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageTypeProto;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeManager;

/**
 * Container placement policy that chooses datanodes with DISK storage.
 *
 * The datanode selection algorithm is {@link SCMContainerPlacementRandom}
 * but only the datanodes with enough {@link StorageTypeProto#DISK} space
 * for data are considered.
 */
public class SCMContainerPlacementHDD extends SCMContainerPlacementRandom {

  /**
   * Construct a random Block Placement policy.
   *
   * @param nodeManager     nodeManager
   * @param conf            Config
   * @param networkTopology
   * @param fallback
   * @param metrics
   */
  public SCMContainerPlacementHDD(NodeManager nodeManager,
                                  ConfigurationSource conf,
                                  NetworkTopology networkTopology,
                                  boolean fallback,
                                  SCMContainerPlacementMetrics metrics) {
    super(nodeManager, conf, networkTopology, fallback, metrics);
  }

  @Override
  protected boolean hasEnoughSpace(DatanodeDetails datanodeDetails,
                                   long metadataSizeRequired,
                                   long dataSizeRequired) {
    return StorageUtils.hasEnoughSpace(datanodeDetails,
                          data ->
                              data.getStorageType() == StorageTypeProto.DISK,
                          null,
                          metadataSizeRequired,
                          dataSizeRequired);
  }
}
