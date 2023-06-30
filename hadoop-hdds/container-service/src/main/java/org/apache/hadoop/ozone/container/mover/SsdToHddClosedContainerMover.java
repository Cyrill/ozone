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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.mover;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.HddVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Policy provides a list of CLOSED containers that stored on SSD volumes
 * and selects DISK labeled ones for moving them to
 */
public class SsdToHddClosedContainerMover extends HddVolumeChoosingPolicy implements ContainerMoverPolicy {

  @Override
  public List<KeyValueContainer> getContainerListToMove(
      ContainerSet containerSet) {
    return containerSet.getContainerMapCopy().values().stream().filter(
        container -> container.getContainerState().equals(ContainerProtos.ContainerDataProto.State.CLOSED)
          && container.getContainerData().getVolume().getStorageType().equals(StorageType.SSD)
    ).map(container -> (KeyValueContainer)container).collect(Collectors.toList());
  }
}
