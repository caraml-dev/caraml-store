package dev.caraml.serving;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CaraMLServing {
  public static void main(String[] args) {
    SpringApplication.run(CaraMLServing.class, args);
  }
}
