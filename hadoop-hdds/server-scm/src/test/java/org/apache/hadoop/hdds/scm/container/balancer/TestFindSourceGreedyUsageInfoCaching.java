/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests to verify that FindSourceGreedy and FindTargetGreedy
 * cache DatanodeUsageInfo to avoid expensive repeated calls
 * to NodeManager.getUsageInfo().
 */
public class TestFindSourceGreedyUsageInfoCaching {

  private NodeManager nodeManager;
  private AtomicInteger getUsageInfoCallCount;
  private ContainerBalancerConfiguration config;
  private List<DatanodeUsageInfo> potentialSources;
  private List<DatanodeUsageInfo> potentialTargets;

  @BeforeEach
  public void setup() {
    getUsageInfoCallCount = new AtomicInteger(0);
    nodeManager = mock(NodeManager.class);

    // Create config with reasonable limits
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    config = ozoneConfig.getObject(ContainerBalancerConfiguration.class);
    config.setMaxSizeLeavingSource(50L * 1024 * 1024 * 1024); // 50GB
    config.setMaxSizeEnteringTarget(50L * 1024 * 1024 * 1024); // 50GB

    // Create potential sources (over-utilized nodes)
    potentialSources = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      // capacity=100GB, used=80GB, remaining=20GB (80% utilized)
      SCMNodeStat stat = new SCMNodeStat(
          100L * 1024 * 1024 * 1024,  // capacity
          80L * 1024 * 1024 * 1024,   // used
          20L * 1024 * 1024 * 1024,   // remaining
          0, 0);
      DatanodeUsageInfo usageInfo = new DatanodeUsageInfo(dn, stat);
      potentialSources.add(usageInfo);

      // Setup mock to count calls and return usage info
      when(nodeManager.getUsageInfo(dn)).thenAnswer(invocation -> {
        getUsageInfoCallCount.incrementAndGet();
        return usageInfo;
      });
    }

    // Create potential targets (under-utilized nodes)
    potentialTargets = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      // capacity=100GB, used=20GB, remaining=80GB (20% utilized)
      SCMNodeStat stat = new SCMNodeStat(
          100L * 1024 * 1024 * 1024,  // capacity
          20L * 1024 * 1024 * 1024,   // used
          80L * 1024 * 1024 * 1024,   // remaining
          0, 0);
      DatanodeUsageInfo usageInfo = new DatanodeUsageInfo(dn, stat);
      potentialTargets.add(usageInfo);

      // Setup mock to count calls and return usage info
      when(nodeManager.getUsageInfo(dn)).thenAnswer(invocation -> {
        getUsageInfoCallCount.incrementAndGet();
        return usageInfo;
      });
    }
  }

  /**
   * Tests that FindSourceGreedy caches DatanodeUsageInfo and doesn't
   * call NodeManager.getUsageInfo() repeatedly in canSizeLeaveSource().
   *
   * Before the fix, each call to canSizeLeaveSource() would call
   * getUsageInfo() which is O(n) due to container set copying.
   * With N containers, this made the overall complexity O(n^2).
   *
   * After the fix, getUsageInfo() is only called during initialization,
   * and canSizeLeaveSource() uses the cached value.
   */
  @Test
  public void testFindSourceGreedyCachesUsageInfo() {
    FindSourceGreedy findSourceGreedy = new FindSourceGreedy(nodeManager);

    // Lower limit at 30% utilization
    Double lowerLimit = 0.3;

    // Initialize - this should NOT call getUsageInfo since we pass
    // DatanodeUsageInfo objects directly
    findSourceGreedy.reInitialize(potentialSources, config, lowerLimit);

    // Reset counter after initialization
    getUsageInfoCallCount.set(0);

    // Simulate checking many containers (like the balancer does)
    // Each container is ~5GB
    long containerSize = 5L * 1024 * 1024 * 1024;
    DatanodeDetails source = potentialSources.get(0).getDatanodeDetails();

    // Call canSizeLeaveSource many times (simulating container iteration)
    int numContainersToCheck = 100;
    for (int i = 0; i < numContainersToCheck; i++) {
      findSourceGreedy.canSizeLeaveSource(source, containerSize);
    }

    // With the caching fix, getUsageInfo should NOT be called
    // during canSizeLeaveSource checks
    assertEquals(0, getUsageInfoCallCount.get(),
        "getUsageInfo() should not be called during canSizeLeaveSource() " +
        "checks - it should use cached value. Called " +
        getUsageInfoCallCount.get() + " times instead of 0.");
  }

  /**
   * Tests that FindTargetGreedy caches DatanodeUsageInfo and doesn't
   * call NodeManager.getUsageInfo() repeatedly.
   */
  @Test
  public void testFindTargetGreedyCachesUsageInfo() {
    FindTargetGreedyByUsageInfo findTargetGreedy =
        new FindTargetGreedyByUsageInfo(null, null, nodeManager);

    // Upper limit at 70% utilization
    Double upperLimit = 0.7;

    // Initialize with potential targets
    findTargetGreedy.reInitialize(potentialTargets, config, upperLimit);

    // Reset counter after initialization
    getUsageInfoCallCount.set(0);

    // Simulate increasing size entering target (like when moves are scheduled)
    DatanodeDetails target = potentialTargets.get(0).getDatanodeDetails();
    long containerSize = 5L * 1024 * 1024 * 1024;

    // Call increaseSizeEntering multiple times
    int numMoves = 5;
    for (int i = 0; i < numMoves; i++) {
      findTargetGreedy.increaseSizeEntering(target, containerSize);
    }

    // With the caching fix, getUsageInfo should NOT be called
    // during increaseSizeEntering - it should use cached value
    assertEquals(0, getUsageInfoCallCount.get(),
        "getUsageInfo() should not be called during increaseSizeEntering() " +
        "- it should use cached value. Called " +
        getUsageInfoCallCount.get() + " times instead of 0.");
  }

  /**
   * Tests that the cache is properly cleared and repopulated on reInitialize.
   */
  @Test
  public void testCacheIsClearedOnReInitialize() {
    FindSourceGreedy findSourceGreedy = new FindSourceGreedy(nodeManager);
    Double lowerLimit = 0.3;

    // First initialization
    findSourceGreedy.reInitialize(potentialSources, config, lowerLimit);

    DatanodeDetails source = potentialSources.get(0).getDatanodeDetails();
    long containerSize = 5L * 1024 * 1024 * 1024;

    // Verify canSizeLeaveSource works
    assertTrue(findSourceGreedy.canSizeLeaveSource(source, containerSize));

    // Create new sources for second initialization
    List<DatanodeUsageInfo> newSources = new ArrayList<>();
    DatanodeDetails newDn = MockDatanodeDetails.randomDatanodeDetails();
    SCMNodeStat newStat = new SCMNodeStat(
        100L * 1024 * 1024 * 1024, 70L * 1024 * 1024 * 1024,
        30L * 1024 * 1024 * 1024, 0, 0);
    newSources.add(new DatanodeUsageInfo(newDn, newStat));

    // Re-initialize with new sources
    findSourceGreedy.reInitialize(newSources, config, lowerLimit);

    // Old source should no longer be in cache - canSizeLeaveSource should
    // return false because there's no record for it
    boolean result = findSourceGreedy.canSizeLeaveSource(source, containerSize);
    assertEquals(false, result,
        "After reInitialize, old sources should not be in cache");

    // New source should work
    assertTrue(findSourceGreedy.canSizeLeaveSource(newDn, containerSize),
        "New source should be in cache after reInitialize");
  }
}
