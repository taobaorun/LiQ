/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.jiaxy.liq.store;

import java.util.ArrayList;
import java.util.List;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/31 10:21
 */
public class GetMessageResult {

    private List<SelectedMappedFileSection> selectedSections = new ArrayList<>();

    private int messageByteSize;

    private GetMessageStatus status;

    private long nextQueueIndex;

    private long queueMinIndex;

    private long queueMaxIndex;


    /**
     * @return message count
     */
    public int getMessageCount() {
        return selectedSections.size();
    }

    public void addMessage(SelectedMappedFileSection messageSection) {
        selectedSections.add(messageSection);
        messageByteSize += messageSection.getSize();
    }


    public List<SelectedMappedFileSection> getSelectedSections() {
        return selectedSections;
    }

    public void setSelectedSections(List<SelectedMappedFileSection> selectedSections) {
        this.selectedSections = selectedSections;
    }

    public int getMessageByteSize() {
        return messageByteSize;
    }

    public void setMessageByteSize(int messageByteSize) {
        this.messageByteSize = messageByteSize;
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getNextQueueIndex() {
        return nextQueueIndex;
    }

    public void setNextQueueIndex(long nextQueueIndex) {
        this.nextQueueIndex = nextQueueIndex;
    }

    public long getQueueMinIndex() {
        return queueMinIndex;
    }

    public void setQueueMinIndex(long queueMinIndex) {
        this.queueMinIndex = queueMinIndex;
    }

    public long getQueueMaxIndex() {
        return queueMaxIndex;
    }

    public void setQueueMaxIndex(long queueMaxIndex) {
        this.queueMaxIndex = queueMaxIndex;
    }
}
