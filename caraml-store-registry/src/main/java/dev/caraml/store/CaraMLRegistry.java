package dev.caraml.store;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CaraMLRegistry {

  public static void main(String[] args) {
    SpringApplication.run(CaraMLRegistry.class, args);
  }
}
