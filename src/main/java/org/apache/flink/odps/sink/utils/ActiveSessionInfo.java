/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.sink.utils;

import com.aliyun.odps.table.write.WriterCommitMessage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ActiveSessionInfo {

    private final Set<Integer> taskIds;
    private final List<WriterCommitMessage> commitMessageList;
    private Long totalRowCount;

    public ActiveSessionInfo() {
        this.taskIds = new HashSet<>();
        this.commitMessageList = new ArrayList<>();
        this.totalRowCount = 0L;
    }

    public void addCommitMessage(WriterCommitMessage commitMessage) {
        commitMessageList.add(commitMessage);
    }

    public void addRowCount(Long rowCount) {
        totalRowCount += rowCount;
    }

    public void addCompleteTask(Integer taskId) {
        taskIds.add(taskId);
    }

    public List<WriterCommitMessage> getCommitMessageList() {
        return commitMessageList;
    }

    public Long getTotalRowCount() {
        return totalRowCount;
    }

    public Set<Integer> getTaskIds() {
        return taskIds;
    }
}