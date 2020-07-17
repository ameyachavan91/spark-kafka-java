package com.demo.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Sender {

	public static void main (String ar[]) throws InterruptedException {
		Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("kafka.topic.name", "test");
        
        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties, new StringSerializer(), new ByteArraySerializer());
        for (int i = 0 ; i < 100; i++) {
        	byte[] payload = (i).getBytes();
        	ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>("test", payload);
        	producer.send(record);
        	Thread.sleep(1000);
        }
        
        producer.close();
	}
	
}
