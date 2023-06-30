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

package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.ratis.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ContainerMoverService extends BackgroundService {

  private final OzoneContainer ozoneContainer;

  private final VolumeChoosingPolicy destVolumeChoosingPolicy =
      new HddVolumeChoosingPolicy();

  private Set<Long> inProgressContainers;

  public ContainerMoverService(OzoneContainer ozoneContainer, long interval,
                               TimeUnit unit,
                               int threadPoolSize, long serviceTimeout) {
    super("ContainerMover", interval, unit, threadPoolSize, serviceTimeout);
    this.ozoneContainer = ozoneContainer;
    inProgressContainers = ConcurrentHashMap.newKeySet();
    constructTmpDir();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue tasksQueue = new BackgroundTaskQueue();
    ozoneContainer.getContainerSet().getContainerMapCopy().values().stream()
        .filter(container -> container.getContainerState().equals(ContainerProtos.ContainerDataProto.State.CLOSED)
            && !inProgressContainers.contains(container.getContainerData().getContainerID()))
        .filter(container -> container.getContainerData().getVolume().getStorageType().equals(StorageType.SSD))
        .forEach(container -> {
          inProgressContainers.add(container.getContainerData().getContainerID());
          tasksQueue.add(new ContainerMoveTask((KeyValueContainer) container, ozoneContainer, destVolumeChoosingPolicy));
        });
    return tasksQueue;
  }

  public boolean isMovingContainer(long containerId) {
    return inProgressContainers.contains(containerId);
  }

  private class ContainerMoveTask implements BackgroundTask {

    private KeyValueContainer container;

    private OzoneContainer ozoneContainer;

    private VolumeChoosingPolicy volumeChoosingPolicy;

    private HddsVolume destVolume;

    public ContainerMoveTask(KeyValueContainer container, OzoneContainer ozoneContainer, VolumeChoosingPolicy volumeChoosingPolicy) {
      this.container = container;
      this.ozoneContainer = ozoneContainer;
      this.volumeChoosingPolicy = volumeChoosingPolicy;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      boolean destVolumeIncreased = false;
      Path diskBalancerTmpDir = null, diskBalancerDestDir = null;
      ContainerData containerData = container.getContainerData();
      long containerSize = containerData.getBytesUsed();
      destVolume = volumeChoosingPolicy.chooseVolume(ozoneContainer.getVolumeSet()
          .getVolumesList().stream().map(storageVolume -> (HddsVolume)storageVolume).collect(Collectors.toList()), container.getContainerData().getMaxSize());
      try {
        diskBalancerTmpDir = Paths.get(destVolume.getTmpDir().getPath())
            .resolve("containerMover").resolve(String.valueOf(container.getContainerData().getContainerID()));

        // Copy container to new Volume's tmp Dir
        ozoneContainer.getController().copyContainer(
            containerData.getContainerType(),
            containerData.getContainerID(), diskBalancerTmpDir);

        // Move container directory to final place on new volume
        String idDir = VersionedDatanodeFeatures.ScmHA.chooseContainerPathID(
            destVolume, destVolume.getClusterID());
        diskBalancerDestDir =
            Paths.get(KeyValueContainerLocationUtil.getBaseContainerLocation(
                destVolume.getHddsRootDir().toString(), idDir,
                containerData.getContainerID()));
        Path destDirParent = diskBalancerDestDir.getParent();
        if (destDirParent != null) {
          Files.createDirectories(destDirParent);
        }
        Files.move(diskBalancerTmpDir, diskBalancerDestDir,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);

        // Generate a new Container based on destDir
        File containerFile = ContainerUtils.getContainerFile(
            diskBalancerDestDir.toFile());
        if (!containerFile.exists()) {
          throw new IOException("ContainerFile for container " + container.getContainerData().getContainerID()
              + " doesn't exists.");
        }
        ContainerData originalContainerData = ContainerDataYaml
            .readContainerFile(containerFile);
        Container newContainer = ozoneContainer.getController()
            .importContainer(originalContainerData, destVolume, diskBalancerDestDir);
        newContainer.getContainerData().getVolume()
            .incrementUsedSpace(containerSize);
        destVolumeIncreased = true;

        // Update container for containerID
        Container oldContainer = ozoneContainer.getContainerSet()
            .getContainer(container.getContainerData().getContainerID());
        oldContainer.writeLock();
        try {
          ozoneContainer.getContainerSet().updateContainer(newContainer);
          oldContainer.delete();
        } finally {
          oldContainer.writeUnlock();
        }
        oldContainer.getContainerData().getVolume()
            .decrementUsedSpace(containerSize);
//        metrics.incrSuccessCount(1);
//        metrics.incrSuccessBytes(containerSize);
      } catch (IOException e) {
        try {
          Files.deleteIfExists(diskBalancerTmpDir);
        } catch (IOException ex) {
          LOG.warn("Failed to delete tmp directory {}", diskBalancerTmpDir,
              ex);
        }
        if (diskBalancerDestDir != null) {
          try {
            Files.deleteIfExists(diskBalancerDestDir);
          } catch (IOException ex) {
            LOG.warn("Failed to delete dest directory {}: {}.",
                diskBalancerDestDir, ex.getMessage());
          }
        }
        // Only need to check for destVolume, sourceVolume's usedSpace is
        // updated at last, if it reaches there, there is no exception.
        if (destVolumeIncreased) {
          destVolume.decrementUsedSpace(containerSize);
        }
//        metrics.incrFailureCount();
      } finally {
        postCall();
      }

      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    private void postCall() {
      inProgressContainers.remove(container.getContainerData().getContainerID());
    }

  }

  private void constructTmpDir() {
    for (HddsVolume volume:
        StorageVolumeUtil.getHddsVolumesList(ozoneContainer.getVolumeSet().getVolumesList())) {
      Path tmpDir = getDiskBalancerTmpDir(volume);
      try {
        FileUtils.deleteFully(tmpDir);
        FileUtils.createDirectories(tmpDir);
      } catch (IOException ex) {
        LOG.warn("Can not reconstruct tmp directory under volume {}", volume,
            ex);
      }
    }
  }

  private Path getDiskBalancerTmpDir(HddsVolume hddsVolume) {
    return hddsVolume.getTmpDir().toPath().resolve("containerMover");
  }

}
