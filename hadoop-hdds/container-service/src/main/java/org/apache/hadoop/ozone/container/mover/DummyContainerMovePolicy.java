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

import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;

import java.util.Collections;
import java.util.List;

/**
 * Container move policy that provides empty list of containers
 * hence the ContainerMoverService will do nothing
 */
public class DummyContainerMovePolicy implements ContainerMoverPolicy {

  @Override
  public List<KeyValueContainer> getContainerListToMove(
      ContainerSet containerSet) {
    return Collections.emptyList();
  }

  @Override
  public HddsVolume chooseVolume(List<HddsVolume> volumes,
                                 long containerMaxSize) {
    return null;
  }
}
