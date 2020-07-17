package com.demo.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Receiver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "test_grp");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        
        try {
        	 
        	KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        	consumer.subscribe(Arrays.asList("test"));
        	
        	for (int i = 0 ; i < 100; i++) {
				ConsumerRecords<String, String> records = consumer.poll(1000);
        		for (ConsumerRecord<String, String> record: records) {
        			System.out.println("Message:: " + record.value());
        		}
        	}
        	
        	consumer.close();
        	
        } catch(Exception e) {
        	e.printStackTrace();
        }
	}

}
