package org.balajiraman.tutorial.kafka.integrations.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	private String consumerKey="";
	private String consumerSecret="";
	private String token="";
	private String secret="";
	List<String> terms = Lists.newArrayList("kafka");
	
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	public TwitterProducer() {

	}
	
	public void run() {
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

		//create a twitter client

		Client client = createTwitterClient(msgQueue);
		client.connect();
		
		//create a Kafka Producer
		KafkaProducer<String, String> producer = createKafkaProducer();
	
		//add a shotdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( ()-> {
			logger.info("stopping application....");
			logger.info("stopping client....");
			client.stop();
			logger.info("stopping producer....");
			producer.close();
			logger.info("done !");

		}
		));
		
		//loop to send tweets to kafka
		// on a different thread, or multiple different threads....
		while (! client.isDone()) {
			
		  String msg = null;
		  try {
			  msg = msgQueue.poll(5, TimeUnit.SECONDS);
		  }catch (InterruptedException e) {
			  e.printStackTrace();
			  client.stop();
		  }
		  if (msg != null) {
			  logger.info("Message Read from Twitter: " + msg);
			  producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
				  @Override
				  public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					  if  (e != null) {
						  logger.error("Failure Occured :" + e);
					  }
				  }
			  }
			  );
			  
		  }
		  logger.info("Application Ended");
		}
	}
	
	public KafkaProducer<String, String> createKafkaProducer(){
		String bootstrapServers = "localhost:9092";
		
		//Create Producer Properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		return producer;
	}
	
	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		
		// Optional: set up some followings and track terms
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
				// Attempts to establish a connection.
		
		return hosebirdClient;
		
	}
	public static void main(String[] args) {
		new TwitterProducer().run();
	}

}
