/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.client.rpc;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_BACKGROUND_PIPELINE_CREATOR_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestCustomReplicationFactor {

  private static MiniOzoneCluster cluster;
  private static OzoneClient ozClient;
  private static ObjectStore store;
  private static OzoneManager ozoneManager;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  private static final String SCM_ID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static File testDir;
  private static OzoneConfiguration conf;

  private static final int MPU_PART_MIN_SIZE = 256 * 1024; // 256KB
  private static final int BLOCK_SIZE = 64 * 1024; // 64KB
  private static final int CHUNK_SIZE = 16 * 1024; // 16KB

  @BeforeAll
  static void init() throws Exception {
    testDir = GenericTestUtils.getTestDir(
              TestSecureOzoneRpcClient.class.getSimpleName());
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setBoolean(HDDS_SCM_BACKGROUND_PIPELINE_CREATOR_ENABLED, false);
    conf.setBoolean(HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK, false);
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    cluster = MiniOzoneCluster.newBuilder(conf)
            .setNumDatanodes(9)
            .setScmId(SCM_ID)
            .setClusterId(CLUSTER_ID)
            .setBlockSize(BLOCK_SIZE)
            .setChunkSize(CHUNK_SIZE)
            .setStreamBufferSizeUnit(StorageUnit.BYTES)
            .build();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
    storageContainerLocationClient = cluster.getStorageContainerLocationClient();
    ozoneManager = cluster.getOzoneManager();
    ozoneManager.setMinMultipartUploadPartSize(MPU_PART_MIN_SIZE);
    TestOzoneRpcClient.setCluster(cluster);
    TestOzoneRpcClient.setOzClient(ozClient);
    TestOzoneRpcClient.setOzoneManager(ozoneManager);
    TestOzoneRpcClient.setStorageContainerLocationClient(storageContainerLocationClient);
    TestOzoneRpcClient.setStore(store);
    TestOzoneRpcClient.setClusterId(CLUSTER_ID);
  }

  @AfterAll
  static void shutdown() throws IOException {
    if (ozClient != null) {
      ozClient.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @MethodSource("bucketConfigs")
  @ParameterizedTest
  void testPutKeyThreeDCs(BucketLayout bucketLayout, int factor) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
            .setBucketLayout(bucketLayout)
            .setDefaultReplicationConfig(
                    new DefaultReplicationConfig(ReplicationConfig.fromTypeAndCustomFactor(factor)))
            .build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);
    createAndVerifyKeyData(bucket, factor);
  }

  static void createAndVerifyKeyData(OzoneBucket bucket, int replicationFactor) throws Exception {
    Instant testStartTime = Instant.now();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    try (OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes(StandardCharsets.UTF_8).length,
          ReplicationConfig.fromTypeAndCustomFactor(replicationFactor),
            new HashMap<>())) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    verifyKeyData(bucket, keyName, value, testStartTime, replicationFactor);

    // Overwrite the key
    try (OzoneOutputStream out = bucket.createKey(keyName,
            value.getBytes(StandardCharsets.UTF_8).length,
            ReplicationConfig.fromTypeAndCustomFactor(replicationFactor),
            new HashMap<>())) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
  }

  static void verifyKeyData(OzoneBucket bucket, String keyName, String value,
           Instant testStartTime, int replicationFactor) throws Exception {
    OzoneKeyDetails key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());

    byte[] fileContent;
    int len;

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      fileContent = new byte[value.getBytes(StandardCharsets.UTF_8).length];
      len = is.read(fileContent);
    }

    assertEquals(len, value.length());
    assertTrue(verifyRatisReplication(bucket.getVolumeName(),
            bucket.getName(), keyName, RATIS, replicationFactor));
    assertEquals(value, new String(fileContent, StandardCharsets.UTF_8));
    assertFalse(key.getCreationTime().isBefore(testStartTime));
    assertFalse(key.getModificationTime().isBefore(testStartTime));
  }

  static boolean verifyRatisReplication(String volumeName, String bucketName,
                                        String keyName, ReplicationType type, int factor) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .build();
    HddsProtos.ReplicationType replicationType =
            HddsProtos.ReplicationType.valueOf(type.toString());
    ReplicationFactor replicationFactor = ReplicationFactor.valueOf(factor);
    HddsProtos.ReplicationFactor protoFactor = ReplicationFactor.toProto(replicationFactor);
    OmKeyInfo keyInfo = ozoneManager.lookupKey(keyArgs);
    for (OmKeyLocationInfo info : keyInfo.getLatestVersionLocations().getLocationList()) {
      ContainerInfo container = storageContainerLocationClient.getContainer(info.getContainerID());
      if (!ReplicationConfig.getLegacyFactor(container.getReplicationConfig())
              .equals(protoFactor) || container.getReplicationType() != replicationType) {
        return false;
      }
    }
    return true;
  }

  private static Stream<Arguments> bucketConfigs() {
    List<Arguments> args = new ArrayList<>();
    int[] customFactors = {2, 4, 5, 7, 8};
    for (BucketLayout layout : BucketLayout.values()) {
      for (int factor : customFactors) {
        args.add(Arguments.of(layout, factor));
      }
    }
    return args.stream();
  }
}
