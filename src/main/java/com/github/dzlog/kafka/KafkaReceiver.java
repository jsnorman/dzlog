package com.github.dzlog.kafka;

import com.gitee.bee.core.conf.BeeConfigClient;
import com.github.dzlog.kafka.consumer.KafkaReceiveHandler;
import com.github.dzlog.service.LogCollectConfigService;
import com.github.dzlog.util.ThreadUtils;
import com.github.dzlog.util.TimeUtils;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.github.dzlog.DzlogConf.DZLOG_DATA_CENTER_CONSUMER_RATE_LIMITER;

@Service
public class KafkaReceiver implements ConsumerSeekAware, ApplicationContextAware,
		BatchConsumerAwareMessageListener<Integer, ByteBuffer>, InitializingBean, DisposableBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiver.class);

	private static final Logger ERR_LOGGER = LoggerFactory.getLogger("errorLogger");

	private static DateTimeFormatter timestampFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

	@Autowired
	private LogCollectConfigService dcLogCollectService;

	//@Autowired
	//protected DcLogMergeService dcLogMergeService;

    @Autowired
	private BeeConfigClient configClient;

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

	private ApplicationContext applicationContext;

	private volatile RateLimiter rateLimiter = null;

	private int lastPermitsPerSecond = 0;

	private String dataCenter;

	private ThreadLocal<KafkaReceiveHandler> receiverHandler = new ThreadLocal<>();

	protected Map<Long, KafkaReceiveHandler> threadToHandlerMap = new ConcurrentHashMap<>();

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		dataCenter = System.getProperty("dataCenter");
		if (StringUtils.isBlank(dataCenter)) {
			throw new IllegalArgumentException("dataCenter can not empty");
		}

		ScheduledExecutorService executorService = ThreadUtils.newDaemonSingleThreadScheduledExecutor("update-rateLimiter");
		executorService.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				Map<String, Integer> map = configClient.getMapInteger(DZLOG_DATA_CENTER_CONSUMER_RATE_LIMITER);
				int permitsPerSecond = map.getOrDefault(dataCenter, -1);
				if (permitsPerSecond > 0) {
					if (rateLimiter != null) {
						if (lastPermitsPerSecond != permitsPerSecond) {
							rateLimiter.setRate(permitsPerSecond);
							ERR_LOGGER.info("[1]?????? RateLimiter???{}", permitsPerSecond);
						}
					} else {
						rateLimiter = RateLimiter.create(permitsPerSecond);
						ERR_LOGGER.info("[2]?????? RateLimiter???{}", permitsPerSecond);
					}
				} else {
					rateLimiter = null;
				}

				lastPermitsPerSecond = permitsPerSecond;
			}
		}, 5, 5, TimeUnit.SECONDS);
	}

	@Override
    @KafkaListener(containerFactory = "dzContainerFactory")
    public void onMessage(List<ConsumerRecord<Integer, ByteBuffer>> list, Consumer<?, ?> consumer) {
        try {
            receiveMessage(list, consumer);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
	}

    @Override
    public void destroy() throws Exception {
		ERR_LOGGER.info("pause consume kafka message");
	    for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
		    container.pause();
	    }

	    TimeUnit.SECONDS.sleep(5);

		ERR_LOGGER.info("prepare to clear remain topic file");
        for (KafkaReceiveHandler handler: threadToHandlerMap.values()) {
            handler.clearRemainTopic();
        }
    }

	private LogEvent createLogEvent(ConsumerRecord<Integer, ByteBuffer> record) {
		if (rateLimiter != null) {
			rateLimiter.acquire();
		}

		try {
			String topic = record.topic();
			if (StringUtils.isBlank(topic)) {
				return null;
			}

			String code = dcLogCollectService.getCodeByTopic(topic);
			if (StringUtils.isBlank(code)) {
				return null;
			}

			LogEvent logEvent = new LogEvent();
			logEvent.setContainHeader(false);

			String receivedTime = TimeUtils.formatTimestamp(record.timestamp());
			logEvent.setReceivedTime(receivedTime);

			ByteBuffer byteBuffer = record.value();
			byteBuffer.position(0);
			logEvent.setMsgByteBuffer(byteBuffer);

			Integer partition = record.partition();
			Long offset = record.offset();

			logEvent.setTopic(topic);
			logEvent.setPartition(partition);
			logEvent.setOffset(offset);
			logEvent.setCode(code);

			return logEvent;
		} catch (Exception e) {
			return null;
		}
	}

	protected void receiveMessage(List<ConsumerRecord<Integer, ByteBuffer>> list, Consumer<?, ?> consumer) {
		KafkaReceiveHandler handler = receiverHandler.get();
		if (handler.getConsumer() == null) {
			handler.setConsumer(consumer);
		}

		String currentHivePartition = TimeUtils.getCurrentDate();
        //??????????????????
		if (!StringUtils.equals(currentHivePartition, handler.getCurrentPartition())) {
			for (String partitionName : handler.getPartitionToTopicConsumerInfoMap().keySet()) {
				handler.flushTopic("new", partitionName);
			}
			//@TODO dcLogMergeService.reduceCount();
			handler.setCurrentPartition(currentHivePartition);
			LOGGER.info("thread {} reduceCount", Thread.currentThread().getId());
		}

		for (ConsumerRecord<Integer, ByteBuffer> record : list) {
			LogEvent logEvent = createLogEvent(record);
			if (logEvent == null || dcLogCollectService.getDcLogPathByCode(logEvent.getCode()) == null) {
				if (logEvent != null) {
                    LOGGER.error("can not get entity of logEvent {} ", logEvent);
				}
				continue;
			}

			String topicPartition = logEvent.getTopicPartition();
			if (handler.appendEvent(logEvent, currentHivePartition)) {
				handler.updateTopicPartition(logEvent);
				if (handler.checkTopicForCommit(topicPartition)) {
					handler.flushTopic("commit", topicPartition);
				}
			} else {
				handler.rollBackTopicPartition(topicPartition);
			}
		}
	}

	/**
	 * kafka ?????????partition ?????????????????????????????????????????????????????????????????????????????????????????????????????????partition?????????
	 * @param assignments
	 * @param consumerSeekCallback
	 */
	@Override
	public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback consumerSeekCallback) {
		long threadId = Thread.currentThread().getId();
		KafkaReceiveHandler handler = receiverHandler.get();
		if (handler != null) {
			IOUtils.closeQuietly(handler);
		} else {
			handler = new KafkaReceiveHandler(consumerSeekCallback, applicationContext);
		}

		handler.setCurrentPartition(TimeUtils.getCurrentDate());
		if (!assignments.isEmpty()) {
			handler.initAssignment(assignments);
		}
		receiverHandler.set(handler);
		threadToHandlerMap.putIfAbsent(threadId, handler);
		ERR_LOGGER.info("thread {} assigned for topic partition {}", threadId, assignments);
	}

	/**
	 * kafka client ?????????????????????????????????????????????????????????????????????kafka offset
	 * @param map
	 * @param consumerSeekCallback
	 */
	@Override
	public void onIdleContainer(Map<TopicPartition, Long> map, ConsumerSeekCallback consumerSeekCallback) {
		String currentPartition = TimeUtils.getCurrentDate();
		Boolean mergeFile = false;
		if (!StringUtils.equals(currentPartition, receiverHandler.get().getCurrentPartition())) {
			mergeFile = true;
		}
		for (String topicPartition : receiverHandler.get().getPartitionToTopicConsumerInfoMap().keySet()) {
			receiverHandler.get().flushTopic("idle", topicPartition);
		}
		if (mergeFile) {
			receiverHandler.get().setCurrentPartition(currentPartition);
			//dcLogMergeService.reduceCount();
			LOGGER.info("thread {} reduceCount", Thread.currentThread().getId());
		}
	}
}
