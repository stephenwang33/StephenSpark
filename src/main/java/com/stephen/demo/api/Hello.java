package com.stephen.demo.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Hello {
	
	@GetMapping("/api")
	public String greeting() {
		return "Hello, Pivotal Cloud Foundry, This is a sample from Stephen";
	}

}
