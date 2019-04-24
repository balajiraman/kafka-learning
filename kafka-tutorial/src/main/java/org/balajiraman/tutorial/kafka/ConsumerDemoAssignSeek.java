package org.balajiraman.tutorial.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
		
		
		String bootstrapServers = "localhost:9092";
		String groupId="my-fourth-application";
		String topics = "first_topic";
		
		//Create Properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

	
		
		//Create Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		
		
		//assign and seek are mostly used to replay data and fetch a specific message
		TopicPartition partitionToReadFrom = new TopicPartition(topics, 1);
		
		//consumer assignment
		consumer.assign(Arrays.asList(partitionToReadFrom));

		//consumer seek
		long offsetToReadFrom = 15L;
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		
		//Consumer Subscription
		//consumer.subscribe(Arrays.asList(topics));
		
		int numberOfMessageToRead = 5;
		boolean keepOnReading = true;
		
		//Poll Messages
		while (keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record: records) {
				logger.info("Key :" + record.key() + ", Value : " + record.value() +", Partition :" + record.partition() + ", Offset :" + record.offset() +", timestamp :" + record.timestamp());
		
				if (numberOfMessageToRead == 0) {
					keepOnReading = false;
					break;
				}else {
					numberOfMessageToRead -= 1;
				}
			}
		}
			
		logger.info("Exiting the application");
	}

}
