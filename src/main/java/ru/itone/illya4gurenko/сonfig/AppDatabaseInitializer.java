package ru.itone.illya4gurenko.сonfig;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.entity.AppAdapterConfig;
import ru.itone.illya4gurenko.repository.AppAdapterConfigRepository;

@Component
@RequiredArgsConstructor
public class AppDatabaseInitializer implements CommandLineRunner {

    private final AppAdapterConfigRepository repository;

    @Getter
    private AppAdapterConfig appAdapterConfig;

    @Override
    public void run(String... args) throws Exception {
        appAdapterConfig = repository.findBySystemId("GRU");
        //тут сконфигуррировать потоки кафки
    }
}
