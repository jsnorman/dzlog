package com.github.dzlog.writer;

import com.github.dzlog.kafka.LogEvent;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author melin 2021/7/19 5:55 下午
 */
public abstract class AbstractFileWriter implements Closeable {

    private Path file;

    // 上次写入的时间
    private long lastWriteTime = System.currentTimeMillis();

    // 当前writer缓存的记录数量
    private long count = 0L;

    // 写入消息总大小
    private long msgBytes = 0L;

    public abstract void write(LogEvent logEvent) throws IOException;

    public void incrementCount() {
        this.count++;
    }

    public void incrementMsgBytes(int msgBytes) {
        this.msgBytes = this.msgBytes + msgBytes;
    }

    public Path getFile() {
        return file;
    }

    public void setFile(Path file) {
        this.file = file;
    }

    public long getLastWriteTime() {
        return lastWriteTime;
    }

    public void setLastWriteTime(long lastWriteTime) {
        this.lastWriteTime = lastWriteTime;
    }

    public long getCount() {
        return count;
    }

    public long getMsgBytes() {
        return msgBytes;
    }
}
