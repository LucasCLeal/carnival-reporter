package com.unicamp.reporter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class ReporterApplication {

	public static void main(String[] args) {
		SpringApplication.run(ReporterApplication.class, args);
		System.out.println("Kafka Stream app up and running!");
	}

	int counter = 0;
	
	@RequestMapping("/")
	public String hello_world(){
		++counter;
		return "Click counter: " + String.valueOf(counter);
		
	}


}
