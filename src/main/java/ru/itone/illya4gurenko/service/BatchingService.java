package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.itone.illya4gurenko.dao.GruVistaDao;
import ru.itone.illya4gurenko.dto.ProducerEventDto;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.*;
import ru.itone.illya4gurenko.repository.GruVistaTabRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class BatchingService {

    private final GruVistaDao gruVistaDao;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ProducerKafkaDto batchAndUpdate(int batchSize){
        List<GruVistaTab> rows = gruVistaDao.fetchBatchForUpdate(batchSize);

        if (rows == null || rows.isEmpty()) {
            return null;
        }

        List<Long> ids = rows.stream()
                .map(GruVistaTab::getId)
                .toList();

        gruVistaDao.updateStatusByIds(ids, FocStatus.IN_PROCESS);

        List<ProducerEventDto> events = new ArrayList<>();
        rows.forEach(row -> {
            row.setFocStatus(FocStatus.IN_PROCESS);
            ProducerEventDto eventDto = new ProducerEventDto()
                    .setId(row.getId())
                    .setSystemAccount(row.getSystemAccount())
                    .setCurrency(row.getCurrency())
                    .setXalfa(row.getXalfa())
                    .setOperation(row.getOperation());
            events.add(eventDto);
        });

        return new ProducerKafkaDto()
                .setActualTimestamp(System.currentTimeMillis())
                .setSystemId(MsgType.GRU)
                .setRequestId(UUID.randomUUID().toString())
                .setEventType(EventType.BALANCE)
                .setEvents(events);
    }
}
