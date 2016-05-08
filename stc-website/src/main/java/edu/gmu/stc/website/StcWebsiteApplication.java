package edu.gmu.stc.website;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class StcWebsiteApplication {

	public static void main(String[] args) {
                WebProperties.initilizeProperties();
		SpringApplication.run(StcWebsiteApplication.class, args);
	}
}
