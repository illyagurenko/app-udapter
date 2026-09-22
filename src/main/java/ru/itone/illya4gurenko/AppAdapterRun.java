package ru.itone.illya4gurenko;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class AppAdapterRun {
    public static void main(String[] args) {
        SpringApplication.run(AppAdapterRun.class, args);
    }
}