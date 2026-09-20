package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.dto.EventDto;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.*;
import ru.itone.illya4gurenko.repository.GruVistaTabRepository;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
@RequiredArgsConstructor
public class BatchingService {

    private final GruVistaTabRepository repository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ConsumerKafkaDto batchAndUpdate(int batchSize){
        List<GruVistaTab> rows = repository.fetchBatchForUpdate(batchSize);

        if (rows.isEmpty()) {
            return null;
        }

        List<Long> ids = rows.stream()
                .map(GruVistaTab::getId)
                .toList();

        repository.updateStatusByIds(ids, FocStatus.IN_PROCESS);
        List<EventDto> events = new ArrayList<>();
        rows.forEach((row) -> {
            row.setFocStatus(FocStatus.IN_PROCESS.name());
            EventDto eventDto = new EventDto()
                    .setId(row.getId())
                    .setSystemAccount(row.getSystemAccount())
                    .setCurrency(row.getCurrency())
                    .setXalfa(row.getXalfa())
                    .setOperation(row.getOperation());
            events.add(eventDto);
        });

        return new ConsumerKafkaDto()
                .setActualTimestamp(System.currentTimeMillis())
                .setSystemId(MsgType.GRU)
                .setReqeustId("req-9f8e7d6c-5b4a")
                .setEventType(EventType.BALANCE)
                .setEntityType(EntityType.ACCOUNT)
                .setEvents(events);
    }
}
