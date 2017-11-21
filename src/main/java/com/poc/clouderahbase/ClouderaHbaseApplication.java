package com.poc.clouderahbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ClouderaHbaseApplication {

	public static void main(String[] args) {
		SpringApplication.run(ClouderaHbaseApplication.class, args);
		new JavaHBase().readData("test");
	}
}
