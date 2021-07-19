package com.github.dzlog.kafka.consumer;

import com.gitee.bee.core.conf.BeeConfigClient;
import com.github.dzlog.entity.LogCollectMetric;
import com.github.dzlog.kafka.LogEvent;
import com.github.dzlog.kafka.TopicStatusInfo;
import com.github.dzlog.service.LogCollectMetricService;
import com.github.dzlog.util.CommonUtils;
import com.github.dzlog.writer.AbstractFileWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.ConsumerSeekAware;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.dzlog.DzlogConf.DZLOG_KAFKA_COMMIT_MAX_INTERVAL_SECONDS;
import static com.github.dzlog.DzlogConf.DZLOG_KAFKA_COMMIT_MAX_NUM;

/**
 * @author melin 2021/7/19 5:15 下午
 */
public class KafkaReceiveHandler implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiveHandler.class);

    private static final Logger ERR_LOGGER = LoggerFactory.getLogger("errorLogger");

    private Consumer consumer = null;

    private ConsumerSeekAware.ConsumerSeekCallback consumerSeekCallback;

    //spring bean start
    private BeeConfigClient configClient;

    private LogCollectMetricService collectMetricService;
    //spring bean end

    /**
     * 当前线程write
     */
    protected String currentPartition = null;

    // topic partition 对应的statusInfo
    protected Map<String, TopicStatusInfo> topicStatusInfoMap = new ConcurrentHashMap<>();

    // topic partition 对应最新消息的 offset`
    protected Map<String, Long> topicPartitionOffsetMap = new ConcurrentHashMap<>();

    // topic partition 对应上一次commit的 offset`
    private Map<String, Long> lastCommitPartitionOffsetMap = new ConcurrentHashMap<>();

    protected static final ConcurrentHashMap<String, Object> CODE_LOCK_MAP = new ConcurrentHashMap<>();

    public KafkaReceiveHandler(ConsumerSeekAware.ConsumerSeekCallback consumerSeekCallback,
                                  ApplicationContext applicationContext) {
        this.consumerSeekCallback = consumerSeekCallback;
        this.configClient = applicationContext.getBean(BeeConfigClient.class);
    }

    @Override
    public void close() throws IOException {
        topicPartitionOffsetMap.clear();
        lastCommitPartitionOffsetMap.clear();
    }

    /**
     * 初始化offset分配，分配thread partition 时调用
     * @param assignments
     */
    public void initAssignment(Map<TopicPartition, Long> assignments) {
        for (TopicPartition partition : assignments.keySet()) {
            long offset = assignments.get(partition);
            String topicPartition = partition.toString();

            topicPartitionOffsetMap.put(topicPartition, offset);
            lastCommitPartitionOffsetMap.put(topicPartition, offset);
        }
    }

    public void flushTopic(String mode, String topicPartition) {
        long threadId = Thread.currentThread().getId();

        // partition 重新分配给其他线程处理。删除遗留未提交的文件。
        if (!topicPartitionOffsetMap.containsKey(topicPartition)) {
            if (topicStatusInfoMap.containsKey(topicPartition)) {
                TopicStatusInfo topicStatusInfo = topicStatusInfoMap.get(topicPartition);

                Set<String> codes = topicStatusInfo.getCollectCodes();
                for (String code : codes) {
                    AbstractFileWriter fileWriter = topicStatusInfo.getFileWriter(code);
                    if (fileWriter == null) {
                        continue;
                    }

                    IOUtils.closeQuietly(fileWriter);
                    topicStatusInfo.removeFileWriter(code);
                    LOGGER.info("[{}] [{}] delete file {}", code, topicPartition, fileWriter.getFile());
                    FileUtils.deleteQuietly(new File(fileWriter.getFile().toString()));
                }
            }
            return;
        }

        if (!topicStatusInfoMap.containsKey(topicPartition)) {
            return;
        }

        AbstractFileWriter currentWriter = null;
        try {
            TopicStatusInfo topicStatusInfo = topicStatusInfoMap.get(topicPartition);

            Boolean isCommit = false;

            for (String code : topicStatusInfo.getCollectCodes()) {
                currentWriter = topicStatusInfo.getFileWriter(code);
                if (currentWriter == null) {
                    continue;
                }

                IOUtils.closeQuietly(currentWriter);
                topicStatusInfo.removeFileWriter(code);

                long count = currentWriter.getCount();
                long msgBytes = currentWriter.getMsgBytes();
                if (count > 0) {
                    long times = updateLocalFile(currentWriter);
                    String file = currentWriter.getFile().toString();

                    if (times > 500) {
                        LOGGER.error("[{}] thread {} prepare to flush topicPartition {} (code:{}), times: {}ms, total count: {}, file: {}",
                                mode, threadId, topicPartition, code, times, count, file);
                    } else if (times > 100) {
                        LOGGER.warn("[{}] thread {} prepare to flush topicPartition {} (code:{}), times: {}ms, total count: {}, file: {}",
                                mode, threadId, topicPartition, code, times, count, file);
                    } else {
                        LOGGER.info("[{}] thread {} prepare to flush topicPartition {} (code:{}), times: {}ms, total count: {}, file: {}",
                                mode, threadId, topicPartition, code, times, count, file);
                    }

                    synchronized (CODE_LOCK_MAP.get(code)) {
                        LogCollectMetric entity = collectMetricService.createEntity(code, topicStatusInfo.getCurrentHivePartition(), count, msgBytes);
                        collectMetricService.recordEntity(entity);
                    }
                    isCommit = true;
                }
            }

            if (isCommit) {
                commitTopic(topicPartition);
            }
        } catch (Exception e) {
            ERR_LOGGER.error("flush topicPartition: " + topicPartition, e);
            if (currentWriter != null) {
                rollBackTopicPartition(topicPartition);
            }
        }
    }

    private void commitTopic(String topicPartition) {
        long threadId = Thread.currentThread().getId();

        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        long offset = topicPartitionOffsetMap.get(topicPartition);
        TopicPartition partition = CommonUtils.createTopicPartition(topicPartition);
        commits.put(partition, new OffsetAndMetadata(offset + 1));
        consumer.commitSync(commits);

        TopicStatusInfo topicStatusInfo = topicStatusInfoMap.get(topicPartition);
        topicStatusInfo.setRecordCount(0L);
        topicStatusInfo.setLastFlushTime(System.currentTimeMillis());
        lastCommitPartitionOffsetMap.put(topicPartition, offset + 1);

        try {
            long offsetIncrement = getOffsetIncrement(topicPartition);
            if (topicStatusInfoMap.containsKey(topicPartition)) {
                long recordCount = topicStatusInfo.getRecordCount();
                if (offsetIncrement - recordCount != 0) {
                    ERR_LOGGER.error("[{}] 线程{}消费数据与写入数据相差 {} 条，offsetIncrement:{}, recordCount: {}",
                            topicPartition, threadId, offsetIncrement - recordCount, offsetIncrement, recordCount);
                }

                if (offsetIncrement - recordCount > 10) {
                    ERR_LOGGER.error("[{}] 线程{}消费数据与写入数据相差 {} 条，offsetIncrement:{}, recordCount: {}",
                            topicPartition, threadId, offsetIncrement - recordCount, offsetIncrement, recordCount);
                }
            }
        } catch (Exception e) {
            ERR_LOGGER.warn("commitTopic failure : {}", e.getMessage());
        }
    }


    /**
     * 获取某个topic的offset增加量
     * @param topicPartition
     * @return
     */
    private long getOffsetIncrement(String topicPartition) {
        long currentOffset = topicPartitionOffsetMap.get(topicPartition);
        long lastOffset = lastCommitPartitionOffsetMap.getOrDefault(topicPartition, currentOffset);
        long offsetIncrement = currentOffset - lastOffset;
        return offsetIncrement + 1;
    }

    protected long updateLocalFile(AbstractFileWriter currentWriter) {
        return 0;
    }

    public void clearRemainTopic() {

    }


    /**
     * 更新topic的状态
     * @param logEvent
     */
    public void updateTopicPartition(LogEvent logEvent) {
        String topicPartition = logEvent.getTopicPartition();
        long offset = logEvent.getOffset();
        topicPartitionOffsetMap.put(topicPartition, offset);
    }

    /**
     * 追加记录至writer
     * @param logEvent
     * @return
     */
    public boolean appendEvent(LogEvent logEvent, String currentHivePartition) {
        String code = logEvent.getCode();
        String topicPartition = logEvent.getTopicPartition();
        try {
            if (!topicStatusInfoMap.containsKey(topicPartition)) {
                TopicStatusInfo topicStatusInfo = new TopicStatusInfo(topicPartition);
                topicStatusInfo.setCurrentHivePartition(currentHivePartition);
                topicStatusInfoMap.put(topicPartition, topicStatusInfo);
                CODE_LOCK_MAP.putIfAbsent(code, new Object());
            }

            TopicStatusInfo topicStatusInfo = topicStatusInfoMap.get(topicPartition);
            topicStatusInfo.setCurrentHivePartition(currentHivePartition);
            if (topicStatusInfo.getFileWriter(code) == null) {
                CODE_LOCK_MAP.putIfAbsent(code, new Object());
            }

            AbstractFileWriter fileWriter = topicStatusInfo.getFileWriter(code);
            if (fileWriter == null) {
                fileWriter = createNewWriter(code, topicPartition, currentHivePartition);
            } else {
                // 如果回滚了offset 清除本地已经写入数据
                long lastoffset = topicPartitionOffsetMap.get(topicPartition);
                if (lastoffset >= logEvent.getOffset()) {
                    if (fileWriter != null) {
                        IOUtils.closeQuietly(fileWriter);
                        topicStatusInfo.removeFileWriter(code);

                        FileUtils.deleteQuietly(new File(fileWriter.getFile().toString()));
                        LOGGER.info("rollback msg, [{}], lastoffset: {}, currentOffset: {}, delete file {}",
                                topicPartition, lastoffset, logEvent.getOffset(), fileWriter.getFile());

                        fileWriter = createNewWriter(code, topicPartition, currentHivePartition);
                    }
                }
            }

            if (fileWriter != null) {
                fileWriter.write(logEvent);
                fileWriter.setLastWriteTime(System.currentTimeMillis());
                fileWriter.incrementCount();
                fileWriter.incrementMsgBytes(logEvent.getMsgBytes());

                topicStatusInfo.incrementRecordCount();
                LOGGER.debug("append event: " + logEvent);
            }

            return true;
        } catch (Exception e) {
            ERR_LOGGER.error("append event, topicPartition: " + topicPartition + ", code: " + code, e);
            return false;
        }
    }

    /**
     * 创建新的writer
     * @param code
     * @param topicPartition
     * @param partition
     * @throws IOException
     */
    protected AbstractFileWriter createNewWriter(String code, String topicPartition, String partition) throws IOException {
        AbstractFileWriter newWriter = createWriter(code, topicPartition, partition);

        if (newWriter != null) {
            TopicStatusInfo topicStatusInfo = topicStatusInfoMap.get(topicPartition);
            topicStatusInfo.setFileWriter(code, newWriter);
        }

        return newWriter;
    }

    private AbstractFileWriter createWriter(String code, String topicPartition, String hivePartition) throws IOException {
        return null;
    }


    /**
     * 检测是否到达提交该topic的时间
     * @param topicPartition
     */
    public boolean checkTopicForCommit(String topicPartition) {
        boolean flush = false;
        if (topicStatusInfoMap.containsKey(topicPartition)) {
            TopicStatusInfo topicStatusInfo = topicStatusInfoMap.get(topicPartition);
            long cachedCount = topicStatusInfo.getRecordCount();
            long lastUpdateTime = topicStatusInfo.getLastFlushTime();
            int commitMaxNum = configClient.getInteger(DZLOG_KAFKA_COMMIT_MAX_NUM);
            int commitMaxIntervalSeconds = configClient.getInteger(DZLOG_KAFKA_COMMIT_MAX_INTERVAL_SECONDS);

            boolean rollBatchCommit = commitMaxNum > 0 && cachedCount >= commitMaxNum;
            long times = System.currentTimeMillis() - lastUpdateTime;
            boolean rollTimeCommit = commitMaxIntervalSeconds > 0 && times >= (commitMaxIntervalSeconds * 1000L);

            flush = rollBatchCommit || rollTimeCommit;

            if (flush) {
                LOGGER.info("topicPartition: {}, commitMaxNum: {}, cachedCount: {}, lastUpdateTime: {}, commitMaxIntervalSeconds: {}, times: {}",
                        commitMaxNum, cachedCount, lastUpdateTime, commitMaxIntervalSeconds, times);
            }
        }

        return flush;
    }

    /**
     * 回滚某个 topicPartition
     *
     * @param topicPartition
     */
    public void rollBackTopicPartition(String topicPartition) {
        LOGGER.error("rollBack topicPartition {}", topicPartition);

        TopicPartition partition = CommonUtils.createTopicPartition(topicPartition);
        long offset = lastCommitPartitionOffsetMap.get(topicPartition);
        consumerSeekCallback.seek(partition.topic(), partition.partition(), offset);

        long threadId = Thread.currentThread().getId();
        LOGGER.info("thread {} seek topicPartition {} to offset {}", threadId, topicPartition, offset);
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    public String getCurrentPartition() {
        return currentPartition;
    }

    public void setCurrentPartition(String currentPartition) {
        this.currentPartition = currentPartition;
    }

    public Map<String, TopicStatusInfo> getTopicStatusInfoMap() {
        return topicStatusInfoMap;
    }

    public void setTopicStatusInfoMap(Map<String, TopicStatusInfo> topicStatusInfoMap) {
        this.topicStatusInfoMap = topicStatusInfoMap;
    }
}
