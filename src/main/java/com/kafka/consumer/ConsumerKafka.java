package com.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Kafka Consumer
 * 
 * @author VENTURA
 *
 */
public class ConsumerKafka {
	
	public static void main(String[] args) {
		
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // for localhost
		// properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
		// "sandbox-hdp.hortonworks.com:6667"); // for hortonworks
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer from Java client");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		Consumer<String, String> consumer = new KafkaConsumer<>(properties);

		consumer.subscribe(Collections.singleton("kafka-test"));

		while (true) {
			//Using for kafka 2.4.0
			//ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			//Only for Kafka  1.1.1
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100).toMillis());


			records.forEach(record -> {
	               System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
	                       record.key(), record.value(),
	                       record.partition(), record.offset());
	           });
	           
			consumer.commitAsync();
		}

	}
}
