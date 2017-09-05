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

package com.jiaxy.liq.core.message;

import com.jiaxy.liq.common.NetUtil;
import com.jiaxy.liq.common.StringUtil;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static com.jiaxy.liq.core.message.MessageType.PADDING;

/**
 * Description: <br/>
 * <p>
 * encode message or decode message
 * <p>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/22 15:39
 */
public class MessageProtocol {

    private static final ByteBuffer msgIdBuffer = ByteBuffer.allocate(MessageProtocol.MESSAGE_ID_LENGTH);

    private static final ByteBuffer hostAddressBuffer = ByteBuffer.allocate(8);

    //0x42414041
    public static final int MAGIC_CODE = 0x434f4c45 & 0x42414241;

    //0x41414C41
    public static final int PADDING_MESSAGE_CODE = 0x434f4c45 & 0x4D414D41;

    public static final int MESSAGE_ID_LENGTH = 16;

    public static final int PADDING_MESSAGE_LENGTH = 8;


    public static void writeMessage(Message message, ByteBuffer byteBuffer) {
        byteBuffer.putInt(message.getMeta().getTotalLength());
        byteBuffer.putInt(MAGIC_CODE);
        byteBuffer.putInt(message.getMeta().getQueueId());
        byteBuffer.putLong(message.getMeta().getQueueOffset());
        byteBuffer.putLong(message.getMeta().getCommitLogOffset());
        byteBuffer.putLong(message.getMeta().getBornTimestamp());
        byteBuffer.putLong(message.getMeta().getStoredTimestamp());
        byteBuffer.put((byte) message.getMeta().getTopicData().length);
        byteBuffer.put(message.getMeta().getTopicData());
        byteBuffer.putInt(message.getData().length);
        byteBuffer.put(message.getData());
    }

    public static void writePaddingMessage(ByteBuffer byteBuffer, long leftSpace) {
        byteBuffer.putInt((int) leftSpace);
        byteBuffer.putInt(PADDING_MESSAGE_CODE);
    }

    public static Message readMessage(ByteBuffer byteBuffer) {
        //read message meta
        MessageMeta meta = readMessageMeta(byteBuffer);
        if (meta.getMsgType() == PADDING_MESSAGE_CODE) {
            return null;
        }
        Message message = new Message(false);
        message.setMeta(meta);
        int dataLength = byteBuffer.getInt();
        byte[] data = new byte[dataLength];
        byteBuffer.get(data);
        message.setData(data);
        return message;
    }


    /**
     * @param byteBuffer
     * @param readData
     * @return
     */
    public static MessageMeta readMessageMeta(ByteBuffer byteBuffer, boolean readData) {
        MessageMeta messageMeta = readMessageMeta(byteBuffer);
        if (readData) {
            int dataLength = byteBuffer.getInt();
            byteBuffer.position(byteBuffer.position() + dataLength);
        }
        return messageMeta;
    }


    /**
     * @param message
     * @param offset  message offset
     * @return
     */
    public static String createMessageId(Message message, long offset) {
        msgIdBuffer.flip();
        hostAddressBuffer.flip();
        msgIdBuffer.limit(MESSAGE_ID_LENGTH);
        msgIdBuffer.put(NetUtil.socketAddress2ByteBuffer((InetSocketAddress) message.getMeta().getStoredHost(),
                hostAddressBuffer));
        msgIdBuffer.putLong(offset);
        return StringUtil.bytesToHex(msgIdBuffer.array());
    }


    public static int calcTotalLength(int dataLength, int topicLength) {
        return 4 + //total length
                4 + //magic code
                4 + //queue id
                8 + //queue index
                8 + //physical offset
                8 + //born timestamp
                8 + //stored timestamp
                1 + //topic length
                topicLength +
                4 + //data length
                dataLength
                ;


    }

    private static MessageMeta readMessageMeta(ByteBuffer byteBuffer) {
        int totalLength = byteBuffer.getInt();
        int code = byteBuffer.getInt();
        MessageMeta meta = new MessageMeta();
        if (PADDING_MESSAGE_CODE == code) {
            meta.setMsgType(PADDING);
            meta.setTotalLength(totalLength);
            return meta;
        }
        int queueId = byteBuffer.getInt();
        long queueIndex = byteBuffer.getLong();
        long phyOffset = byteBuffer.getLong();
        long bornTimestamp = byteBuffer.getLong();
        long storedTimestamp = byteBuffer.getLong();
        int topicLength = byteBuffer.get();
        byte[] topicData = new byte[topicLength];
        byteBuffer.get(topicData);
        String topic = new String(topicData);
        meta.setQueueId(queueId);
        meta.setQueueOffset(queueIndex);
        meta.setCommitLogOffset(phyOffset);
        meta.setTopic(topic);
        meta.setTotalLength(totalLength);
        meta.setBornTimestamp(bornTimestamp);
        meta.setStoredTimestamp(storedTimestamp);
        return meta;

    }
}
