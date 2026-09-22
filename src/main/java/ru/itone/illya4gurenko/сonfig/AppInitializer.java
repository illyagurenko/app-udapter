package ru.itone.illya4gurenko.сonfig;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.dao.AppAdapterConfigDao;
import ru.itone.illya4gurenko.entity.AppAdapterConfig;
import ru.itone.illya4gurenko.repository.AppAdapterConfigRepository;
import ru.itone.illya4gurenko.service.GruConsumer;
import ru.itone.illya4gurenko.service.GruProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
@Slf4j
public class AppInitializer implements CommandLineRunner {

    private final AppAdapterConfigDao configDao;
    private final ApplicationContext applicationContext;

    @Value("${batch.timeout.worker.delay:10000}")
    private Long delay;

    @Value("${app.adapter.producer.pool-size:1}")
    private int producerPoolSize;

    @Value("${app.adapter.consumer.pool-size:1}")
    private int consumerPoolSize;

    private ExecutorService producerExecutor;

    private final List<GruProducer> producers = new ArrayList<>();
    private final List<GruConsumer> consumers = new ArrayList<>();

    @Override
    public void run(String... args) throws Exception {
        AppAdapterConfig config = configDao.getBySystemId("GRU"); // Обращение через DAO
        if (config == null) {
            log.error("AppAdapterConfig for systemId 'GRU' not found!");
            return;
        }

        initProducers(config);
        initConsumers(config);
    }

    private void initProducers(AppAdapterConfig config) {
        if (!Long.valueOf(1L).equals(config.getStatusOut()) || config.getTopicOut() == null) {
            log.warn("Producer is disabled or topicOut is null. Skipping init.");
            return;
        }

        log.info("Initializing {} Producers...", producerPoolSize);

        producerExecutor = Executors.newFixedThreadPool(producerPoolSize, r -> {
            Thread t = new Thread(r);
            t.setName("producer-worker-thread");
            t.setDaemon(true);
            return t;
        });

        for (int i = 0; i < producerPoolSize; i++) {
            GruProducer producer = applicationContext.getBean(GruProducer.class);
            producer.init(config.getTopicOut(), delay, i + 1);
            producers.add(producer);
            producerExecutor.submit(producer);
        }
    }

    private void initConsumers(AppAdapterConfig config) {
        if (!Long.valueOf(1L).equals(config.getStatusIn()) || config.getTopicIn() == null) {
            log.warn("Consumer is disabled or topicIn is null. Skipping init.");
            return;
        }

        log.info("Initializing {} Consumers...", consumerPoolSize);

        for (int i = 0; i < consumerPoolSize; i++) {
            GruConsumer consumer = applicationContext.getBean(GruConsumer.class);
            consumer.start(config.getTopicIn(), i + 1);
            consumers.add(consumer);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down application, stopping producers and consumers...");

        producers.forEach(GruProducer::stop);
        if (producerExecutor != null) {
            producerExecutor.shutdownNow();
        }

        consumers.forEach(GruConsumer::stop);
    }
}