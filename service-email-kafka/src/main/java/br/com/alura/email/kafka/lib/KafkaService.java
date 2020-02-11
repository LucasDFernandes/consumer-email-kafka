package br.com.alura.email.kafka.lib;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.alura.email.kafka.deserializer.GsonDeserializer;

public class KafkaService<T> implements Closeable {

	private final KafkaConsumer<String, T> consumer;
	private final ConsumerFunction<T> parse;

	private KafkaService(String groupConsumer, ConsumerFunction<T> parse, Class<T> type, Map<String, String> map) {
		this.consumer = new KafkaConsumer<String, T>(getProperties(type, groupConsumer, map));
		this.parse = parse;
	}

	public KafkaService(String groupConsumer, String topic, ConsumerFunction<T> parse, Class<T> type) {
		this(groupConsumer, parse, type, null);
		consumer.subscribe(Collections.singletonList(topic));
	}

	public KafkaService(String groupConsumer, Pattern pattern, ConsumerFunction<T> parse, Class<T> type,
			Map<String, String> map) {
		this(groupConsumer, parse, type, map);
		consumer.subscribe(pattern);
	}

	public void run() {
		while (true) {
			ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
			if (!records.isEmpty()) {
				System.out.println("encontrei " + records.count() + " registros");
				for (ConsumerRecord<String, T> record : records) {
					parse.consume(record);
				}
			}
		}
	}

	/**
	 * 
	 * Configuração das propiedades do Kafka
	 * 
	 * @param type
	 * 
	 * @param groupConsumer
	 * 
	 * @return
	 */
	private Properties getProperties(Class<T> type, String groupConsumer, Map<String, String> overridePropertiers) {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupConsumer);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
		if (overridePropertiers != null) {
			properties.putAll(overridePropertiers);
		}
		return properties;
	}

	@Override
	public void close() {
		consumer.close();
	}

}
