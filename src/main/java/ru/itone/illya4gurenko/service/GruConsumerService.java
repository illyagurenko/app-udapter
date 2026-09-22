package ru.itone.illya4gurenko.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.itone.illya4gurenko.dao.AppAdapterIoMsgsDao;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;
import ru.itone.illya4gurenko.dao.GruRejectDao;
import ru.itone.illya4gurenko.dao.GruVistaDao;
import ru.itone.illya4gurenko.dto.*;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.GruRejectTab;
import ru.itone.illya4gurenko.entity.enums.Dir;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.entity.enums.MsgType;
import ru.itone.illya4gurenko.repository.AppAdapterIoMsgsRepository;
import ru.itone.illya4gurenko.repository.AppAdapterTransRepository;
import ru.itone.illya4gurenko.repository.GruRejectTabRepository;
import ru.itone.illya4gurenko.repository.GruVistaTabRepository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class GruConsumerService {

    @Value("${pc.number}")
    private Long numberPC;

    private final ObjectMapper objectMapper;
    private final AppAdapterTransDao transDao;
    private final AppAdapterIoMsgsDao ioMsgsDao;
    private final GruVistaDao gruVistaDao;
    private final GruRejectDao gruRejectDao;

    @Transactional
    public void consume(String json) {
        log.info("consume json:\n{}", json);
        ConsumerKafkaDto consumerKafkaDto;
        try {
            consumerKafkaDto = objectMapper.readValue(json, ConsumerKafkaDto.class);
        } catch (Exception e) {
            log.error("error parse json: {}", json, e);
            return;
        }

        String requestId = consumerKafkaDto.getRequestId();
        log.info("process response for requestId={}", requestId);

        AppAdapterTrans trans = transDao.findByRequestId(requestId);
        if (trans == null) {
            log.error("trans with requestId='{}' not found", requestId);
            return;
        }

        AppAdapterIoMsgs ioMsg = new AppAdapterIoMsgs()
                .setTransId(trans.getId())
                .setMsgType(MsgType.GRU)
                .setDir(Dir.IN)
                .setMsg(json)
                .setInsTs(LocalDateTime.now())
                .setNodeId(numberPC);
        ioMsgsDao.save(ioMsg);

        ProducerKafkaDto sentDto;
        try {
            sentDto = objectMapper.readValue(trans.getData(), ProducerKafkaDto.class);
        } catch (Exception e) {
            log.error("error parse sent trans.data for transId={}", trans.getId(), e);
            return;
        }

        if ("ERROR".equalsIgnoreCase(consumerKafkaDto.getStatus()) || consumerKafkaDto.getEvents() == null) {
            log.warn("all batch with status ERROR");
            String errorReason = consumerKafkaDto.getError() != null ? consumerKafkaDto.getError().getMessage() : "Общая ошибка пачки";
            rejectEntireBatch(trans, sentDto, errorReason);
            return;
        }

        Set<Long> sentIds = sentDto.getEvents().stream()
                .map(ProducerEventDto::getId)
                .collect(Collectors.toSet());

        Set<Long> receivedEntityIds = consumerKafkaDto.getEvents().stream()
                .map(ConsumerEventDto::getEntityId)
                .collect(Collectors.toSet());

        if (!receivedEntityIds.containsAll(sentIds)) {
            log.error("post not all ENTITY_ID post: {}, get: {}", sentIds, receivedEntityIds);
            rejectEntireBatch(trans, sentDto, "not all rows");
            return;
        }

        for (ConsumerEventDto event : consumerKafkaDto.getEvents()) {
            Long entityId = event.getEntityId();

            if ("SUCCESS".equalsIgnoreCase(event.getStatus().name())) {
                EventDataDto data = event.getData();
                gruVistaDao.updateBalanceAndStatus(
                        entityId,
                        data != null ? data.getOldTbal() : null,
                        data != null ? data.getNewTbal() : null,
                        FocStatus.SUCCESS
                );
            } else {
                gruVistaDao.updateStatusByIds(List.of(entityId), FocStatus.ERROR);

                String errorMsg = event.getError() != null
                        ? (event.getError().getCode() + ": " + event.getError().getMessage())
                        : "Unknown error";

                GruRejectTab reject = new GruRejectTab()
                        .setVistaTabId(entityId)
                        .setSystemAccount(event.getEntityValue())
                        .setRejectDesc(errorMsg)
                        .setFrontStatus("ERR")
                        .setFrontTimestamp(LocalDateTime.now());
                gruRejectDao.save(reject);
            }
        }

        trans.setStatus(FocStatus.SUCCESS);
        trans.setRespCode("0");
        trans.setRespDesc("SUCCESS");
        transDao.updateStatus(trans);
        log.info("success consume");
    }

    private void rejectEntireBatch(AppAdapterTrans trans, ProducerKafkaDto sentDto, String reason) {
        log.warn("all batch error: {}. requestId={}", reason, trans.getRequestId());

        List<Long> sentIds = sentDto.getEvents().stream()
                .map(ProducerEventDto::getId)
                .toList();


        gruVistaDao.updateStatusByIds(sentIds, FocStatus.ERROR);

        for (ProducerEventDto event : sentDto.getEvents()) {
            GruRejectTab reject = new GruRejectTab()
                    .setVistaTabId(event.getId())
                    .setSystemAccount(event.getSystemAccount())
                    .setRejectDesc(reason)
                    .setFrontStatus("ERR")
                    .setFrontTimestamp(LocalDateTime.now());
            gruRejectDao.save(reject);
        }

        trans.setStatus(FocStatus.ERROR);
        trans.setRespDesc(reason);
        transDao.updateStatus(trans);
    }
}