/**
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

package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.FilterOutputStream;
import java.io.OutputStream;

/**
 * This stream is used to access the underlying {@link KeyOutputStream} stream.
 * A wrapper for a {@link CompressionOutputStream} that also stores
 * the original stream.
 */
public class CompressedOutputStream extends FilterOutputStream {
  private final OutputStream wrapped;

  public CompressedOutputStream(OutputStream out, OutputStream wrapped) {
    super(out);
    this.wrapped = wrapped;
  }

  public OutputStream getWrappedStream() {
    return wrapped;
  }
}
