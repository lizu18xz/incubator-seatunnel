/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.sink.ftp.filesystem;

import org.apache.seatunnel.connectors.seatunnel.file.sink.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.ftp.util.FtpFileUtils;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystemCommitter;

import lombok.NonNull;

import java.io.IOException;
import java.util.Map;

public class FtpFileSystemCommitter implements FileSystemCommitter {
    @Override
    public void commitTransaction(@NonNull FileAggregatedCommitInfo aggregateCommitInfo) throws IOException {
        for (Map.Entry<String, Map<String, String>> entry :
                aggregateCommitInfo.getTransactionMap().entrySet()) {
            for (Map.Entry<String, String> mvFileEntry : entry.getValue().entrySet()) {
                String key = mvFileEntry.getKey();
                String value = mvFileEntry.getValue();
                FtpFileUtils.renameFile(key, value);
            }
            FtpFileUtils.deleteFiles(entry.getKey());
        }
    }

    @Override
    public void abortTransaction(@NonNull FileAggregatedCommitInfo aggregateCommitInfo) throws IOException {
        for (Map.Entry<String, Map<String, String>> entry :
                aggregateCommitInfo.getTransactionMap().entrySet()) {
            for (Map.Entry<String, String> mvFileEntry : entry.getValue().entrySet()) {
                String oldFile = mvFileEntry.getKey();
                String newFile = mvFileEntry.getValue();
                if (FtpFileUtils.fileExist(newFile) && !FtpFileUtils.fileExist(oldFile)) {
                    FtpFileUtils.renameFile(mvFileEntry.getValue(), mvFileEntry.getKey());
                }
            }
            FtpFileUtils.deleteFiles(entry.getKey());
        }
    }
}
