package ru.itone.illya4gurenko.сonfig;

import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.entity.AppAdapterConfig;
import ru.itone.illya4gurenko.repository.AppAdapterConfigRepository;
import ru.itone.illya4gurenko.service.GruVistaTabProducerService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@RequiredArgsConstructor
@Slf4j
public class AppInitializer implements CommandLineRunner {

    @Value("${batch.timeout.worker.delay}")
    private Long delay;

    private final AppAdapterConfigRepository repository;
    private final ConsumerFactory<String, String> consumerFactory;
    private final GruVistaTabProducerService gruVistaTabProducerService;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private ExecutorService producerExecutor;
    private KafkaMessageListenerContainer<String, String> consumerContainer;

    @Getter
    private AppAdapterConfig config;

    @Override
    public void run(String... args) throws Exception {
        config = repository.findBySystemId("GRU");
        if (config == null) {
            log.error("config not found");
            return;
        }

        initProducerThread(config);
        initConsumerThread(config);
    }

    private void initProducerThread(AppAdapterConfig config) {
        if (!Long.valueOf(1L).equals(config.getStatusOut()) || config.getTopicOut() == null) {
            log.warn("error init producer");
            return;
        }

        final String topicOut = config.getTopicOut();
        log.info("init producer in topic: {}", topicOut);

        producerExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "producer-vista-thread");
            t.setDaemon(true);
            return t;
        });

        producerExecutor.submit(() -> {
            log.info("producer running");
            while (isRunning.get()) {
                try {
                    boolean hasProcessedData = gruVistaTabProducerService.processOneBatch(topicOut);

                    if (!hasProcessedData) {
                        Thread.sleep(delay);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.info("thread interrupt");
                    break;
                } catch (Exception e) {
                    log.error("tech error", e);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        });
    }

    private void initConsumerThread(AppAdapterConfig config) {
        if (!Long.valueOf(1L).equals(config.getStatusIn()) || config.getTopicIn() == null) {
            log.warn("error init consumer");
            return;
        }

        final String topicIn = config.getTopicIn();
        log.info("read topic: {}", topicIn);

        ContainerProperties properties = new ContainerProperties(topicIn);
        properties.setMessageListener((MessageListener<String, String>) record -> {
            log.info("get message: key={}, value={}", record.key(), record.value());
            // читаем топик выпоняем данные с ним
        });

        consumerContainer = new KafkaMessageListenerContainer<>(consumerFactory, properties);
        consumerContainer.setBeanName("consumer-vista-container");
        consumerContainer.start();
    }

    @PreDestroy
    public void shutdown() {
        isRunning.set(false);
        if (producerExecutor != null) {
            producerExecutor.shutdownNow();
        }
        if (consumerContainer != null) {
            consumerContainer.stop();
        }
    }
}
