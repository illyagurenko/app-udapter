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
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class BatchingService {

    private final GruVistaDao gruVistaDao;
    private final AdapterEntityFactory adapterEntityFactory;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ProducerKafkaDto batchAndUpdate(int batchSize) {
        List<GruVistaTab> rows = gruVistaDao.fetchBatchForUpdate(batchSize);

        if (rows == null || rows.isEmpty()) {
            return null;
        }

        List<Long> ids = rows.stream()
                .map(GruVistaTab::getId)
                .toList();

        gruVistaDao.updateStatusByIds(ids, FocStatus.IN_PROCESS);

        return adapterEntityFactory.createProducerKafkaDto(rows);
    }
}
