package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.itone.illya4gurenko.dao.AppAdapterIoMsgsDao;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.enums.Dir;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;
import ru.itone.illya4gurenko.utils.MapperUtils;

@Service
@RequiredArgsConstructor
@Slf4j
public class GruConsumerService {

    @Value("${pc.number}")
    private Long numberPC;

    private final MapperUtils mapperUtils;
    private final AppAdapterTransDao transDao;
    private final AppAdapterIoMsgsDao ioMsgsDao;
    private final AdapterEntityFactory adapterEntityFactory;
    private final GruValidateService gruValidateService;

    @Transactional
    public void consume(String json) {
        log.info("Consume json:\n{}", json);

        ConsumerKafkaDto consumerDto = mapperUtils.tryMappingToConsumerKafkaDto(json);
        if (consumerDto == null) return;

        String requestId = consumerDto.getRequestId();
        AppAdapterTrans trans = transDao.findByRequestId(requestId);
        if (trans == null) {
            log.error("Trans with requestId='{}' not found", requestId);
            return;
        }

        if (trans.getStatus() == FocStatus.SUCCESS || trans.getStatus() == FocStatus.ERROR) {
            log.warn("Message with requestId='{}' already processed with status: {}. Skipping duplicate.",
                    requestId, trans.getStatus());
            return;
        }

        AppAdapterIoMsgs ioMsg = adapterEntityFactory.createIoMsg(trans.getId(), Dir.IN, json, numberPC);
        ioMsgsDao.save(ioMsg);

        ProducerKafkaDto sentDto = mapperUtils.tryMappingToProducerKafkaDto(trans.getData());
        if (sentDto == null) {
            trans.setStatus(FocStatus.ERROR).setRespDesc("Failed to parse sent data");
            transDao.updateStatus(trans);
            return;
        }

        try {
            gruValidateService.processResult(consumerDto, sentDto);

            if ("ERROR".equalsIgnoreCase(consumerDto.getStatus())) {
                trans.setStatus(FocStatus.ERROR).setRespDesc("Batch error processed");
            } else {
                trans.setStatus(FocStatus.SUCCESS).setRespCode("0").setRespDesc("SUCCESS");
            }
        } catch (Exception e) {
            log.error("Error processing business logic", e);
            trans.setStatus(FocStatus.ERROR).setRespDesc(e.getMessage());
        }

        transDao.updateStatus(trans);
    }
}