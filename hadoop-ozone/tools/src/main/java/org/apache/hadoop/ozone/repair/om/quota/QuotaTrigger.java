/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.repair.om.quota;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

/**
 * CLI to trigger quota repair.
 */
@CommandLine.Command(name = "start",
    description = "Trigger quota repair for all or specific buckets.")
public class QuotaTrigger implements Callable<Void> {

  @CommandLine.ParentCommand
  private QuotaRepair parent;

  @CommandLine.Option(
      names = {"--service-id", "--om-service-id"},
      description = "Ozone Manager Service ID",
      required = false
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"--service-host"},
      description = "Ozone Manager Host. If OM HA is enabled, use --service-id instead. "
          + "If you must use --service-host with OM HA, this must point directly to the leader OM. "
          + "This option is required when --service-id is not provided or when HA is not enabled."
  )
  private String omHost;

  @CommandLine.Option(names = {"--buckets"},
      required = false,
      description = "Start quota repair for specific buckets. Input will be a list of URIs separated by comma as " +
          "/<volume>/<bucket>[,...]")
  private String buckets;

  @Override
  public Void call() throws Exception {
    List<String> bucketList = Collections.emptyList();
    if (StringUtils.isNotEmpty(buckets)) {
      bucketList = Arrays.asList(buckets.split(","));
    }

    try (OzoneManagerProtocol omClient = parent.createOmClient(omServiceId, omHost, false)) {
      System.out.println("Triggering quota repair for " +
          (bucketList.isEmpty() ? "all buckets" : "buckets " + buckets));
      omClient.startQuotaRepair(bucketList);
      System.out.println(omClient.getQuotaRepairStatus());
    }
    return null;
  }
}
