package br.com.alura.email.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import br.com.alura.email.kafka.lib.KafkaService;
import br.com.alura.email.kafka.service.EmailService;

@SpringBootApplication
public class ServiceEmailKafkaApplication {
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) {
		EmailService emailService = new EmailService();
		try (KafkaService service = new KafkaService(EmailService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL",
				emailService::parse, String.class)) {
			service.run();
		}
		
		SpringApplication.run(ServiceEmailKafkaApplication.class, args);
	}

}
