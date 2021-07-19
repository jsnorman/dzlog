package com.github.dzlog.kafka;

import com.github.dzlog.writer.AbstractFileWriter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by taofu on 2018/1/11.
 */
public class TopicStatusInfo {

	private String topic;

	private String currentHivePartition;

	private long recordCount = 0;

	private long lastFlushTime = 0;

	/**
	 * 采集code -> AbstractFileWriter
	 */
	private Map<String, AbstractFileWriter> fileWriterMap = new HashMap<>();

	public TopicStatusInfo(String topic) {
		this.topic = topic;
	}

	public void incrementRecordCount() {
        recordCount++;
    }

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getCurrentHivePartition() {
		return currentHivePartition;
	}

	public void setCurrentHivePartition(String currentHivePartition) {
		this.currentHivePartition = currentHivePartition;
	}

	public long getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(long recordCount) {
		this.recordCount = recordCount;
	}

	public long getLastFlushTime() {
		return lastFlushTime;
	}

	public void setLastFlushTime(long lastFlushTime) {
		this.lastFlushTime = lastFlushTime;
	}

	public Set<String> getCollectCodes() {
		return fileWriterMap.keySet();
	}

	public AbstractFileWriter getFileWriter(String code) {
		return fileWriterMap.get(code);
	}

	public void setFileWriter(String code, AbstractFileWriter fileWriter) {
		fileWriterMap.put(code, fileWriter);
	}

	public void removeFileWriter(String code) {
		fileWriterMap.remove(code);
	}
}
