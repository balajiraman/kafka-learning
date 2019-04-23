package org.balajiraman.tutorial.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {

	static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		String bootstrapServers = "localhost:9092";

		// Create Producer Properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		// Create Producer Record
		
		for (int messageIndex = 0; messageIndex < 10; messageIndex++) {
		
			String topic = "first_topic";
			String message = "Hello World Message !";
			String key = "id_"+messageIndex; 

			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message + ": "+messageIndex);

			// Send Data -- asynchronously
			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					// executes every time a record is successfully sent or an exception is thrown
					if (e == null) {
						// the record is sent successfully.
						logger.info("Produced Message::" + "Topic :" + recordMetadata.topic() + ", Id: " + key + ", Partition: "
								+ recordMetadata.partition() + ", Offset: " + recordMetadata.offset() + ", timestamp :"
								+ recordMetadata.timestamp());

					} else {
						logger.error("Error while producting", e);
					}
				}
			});
			//.get(); //.get() Makes the call synchronous and throws exceptions that need to be handled.
						
			producer.flush();
		};

		producer.close();
	}

}
