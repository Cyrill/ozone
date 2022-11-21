/**
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

package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.FilterInputStream;
import java.io.IOException;

/**
 * A CompressionInputStream for Ozone with length. This stream is used to read
 * Parts of a MPU Key in Compressed Buckets.
 */
public class OzoneCompressedInputStream extends FilterInputStream
    implements EncodedInputStream {

  private final CompressionInputStream in;
  private final long length;

  public OzoneCompressedInputStream(CompressionInputStream in, long length) {
    super(in);
    this.in = in;
    this.length = length;
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public void unbuffer() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void seek(long pos) throws IOException {
    in.seek(pos);
  }

  @Override
  public long getPos() throws IOException {
    return in.getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) {
    return in.seekToNewSource(targetPos);
  }
}
