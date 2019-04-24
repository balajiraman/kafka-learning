package org.balajiraman.tutorial.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThreads {

	ConsumerDemoWithThreads() {
		
	}
	private void run() {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

		CountDownLatch latch = new CountDownLatch(1);
		String bootstrapServers = "localhost:9092";
		String groupId = "my-sixth-application";
		String topics = "first_topic";

		logger.info("Creating Runnable Thread :");
		Runnable myConsumerRunnable = new ConsumerRunnable (bootstrapServers, groupId, topics, latch);
		//Start the Thread
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			logger.info("Caught Shutdown hook");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			}catch (InterruptedException e) {
				logger.info("Application has Exited");
			}
		}
		));
		
		try {
			latch.await();
		}catch(InterruptedException e) {
			e.printStackTrace();
			logger.error("application got interrupted" + e);
		}finally {
			logger.info("Thread Shuttig down...");
		}
	}
	public static void main(String[] args) { 
		new ConsumerDemoWithThreads().run();
	}


	
	public class ConsumerRunnable implements Runnable {

		private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

		private CountDownLatch latch;
		private String bootstrapServers = "localhost:9092";
		private String groupId = "my-fourth-application";
		private String topics = "first_topic";

		private KafkaConsumer<String, String> consumer;
		// Create Properties
		Properties properties = new Properties();

		public ConsumerRunnable (String bootstrapServers, String groupId, String topics, CountDownLatch latch) {
			this.latch = latch;
			this.groupId = groupId;
			this.topics = topics;
			this.bootstrapServers = bootstrapServers;

			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		}

		public void run() {
			//Create Consumer
			consumer = new KafkaConsumer<>(properties);
			
			//Consumer Subscription
			consumer.subscribe(Arrays.asList(topics));
			
			//Poll Messages
			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
		
					for (ConsumerRecord<String, String> record: records) {
						logger.info("Key :" + record.key() + ", Value : " + record.value() +", Partition :" + record.partition() + ", Offset :" + record.offset() +", timestamp :" + record.timestamp());
					}
				}
			}catch (WakeupException e) {
				logger.info("Received Shutdown Signal!");
				
			}finally{
				consumer.close();
				//tell our parent thread we are done.
				latch.countDown();
			}
		}

		public void shutdown() {
			consumer.wakeup();
		}
	}

}
