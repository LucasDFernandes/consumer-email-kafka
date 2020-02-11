package br.com.alura.email.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
public class EmailService {

	public void parse(ConsumerRecord<String, String> record) {
		System.out.println("---------------------------------------------");
		System.out.println("Sending email");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Email sended");
		System.out.println("---------------------------------------------");
	}

}
