package com.github.dzlog.kafka;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author melin 2021/7/19 5:16 下午
 */
public class LogEvent {
    private String msgId;

    private String topic;

    private Integer partition;

    private Long offset;

    private String code;

    private String receivedTime;

    private ByteBuffer msgByteBuffer;

    private boolean containHeader;

    private int msgBytes;

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getTopic() {
        return topic;
    }

    public String getTopicPartition() {
        return topic + "-" + partition;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getReceivedTime() {
        return receivedTime;
    }

    public void setReceivedTime(String receivedTime) {
        this.receivedTime = receivedTime;
    }

    public ByteBuffer getMsgByteBuffer() {
        return msgByteBuffer;
    }

    public void setMsgByteBuffer(ByteBuffer msgByteBuffer) {
        this.msgBytes = msgByteBuffer.remaining();
        this.msgByteBuffer = msgByteBuffer;
    }

    public boolean isContainHeader() {
        return containHeader;
    }

    public void setContainHeader(boolean containHeader) {
        this.containHeader = containHeader;
    }

    public int getMsgBytes() {
        return msgBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogEvent logEvent = (LogEvent) o;
        return Objects.equals(msgId, logEvent.msgId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgId, code, partition, offset);
    }

    @Override
    public String toString() {
        return "LogEvent{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", code='" + code + '\'' +
                ", receivedTime='" + receivedTime + '\'' +
                '}';
    }
}
