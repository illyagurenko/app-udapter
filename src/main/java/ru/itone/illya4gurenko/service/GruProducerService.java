package ru.itone.illya4gurenko.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.enums.Dir;
import ru.itone.illya4gurenko.entity.enums.EventType;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.entity.enums.MsgType;
import ru.itone.illya4gurenko.repository.AppAdapterIoMsgsRepository;
import ru.itone.illya4gurenko.repository.AppAdapterTransRepository;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class GruProducerService {

    @Value("${batch.size}")
    private int batchSize;

    @Value("${pc.number}")
    private Long nodeId;

    private final BatchingService batchingService;
    private final AppAdapterTransRepository appAdapterTransRepository;
    private final AppAdapterIoMsgsRepository appAdapterIoMsgsRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public boolean produce(String topicOut) {
        ProducerKafkaDto curDto = batchingService.batchAndUpdate(batchSize);
        if (curDto == null || curDto.getEvents() == null || curDto.getEvents().isEmpty()) {
            log.trace("not rows with status WAIT");
            return false;
        }

        log.info("found batch {} rows requestId={}", curDto.getEvents().size(), curDto.getRequestId());

        String json;
        try {
            json = objectMapper.writeValueAsString(curDto);
            log.error("success mapping\n{}", json);
        } catch (Exception e) {
            log.error("error mapping", e);
            return false;
        }

        AppAdapterTrans trans = new AppAdapterTrans()
                .setSystemId(MsgType.GRU)
                .setRequestId(curDto.getRequestId())
                .setEventType(EventType.BALANCE)
                .setData(json)
                .setStatus(FocStatus.IN_PROCESS)
                .setInsTs(LocalDateTime.now());
        appAdapterTransRepository.save(trans);

        try {
            SendResult<String, String> res = kafkaTemplate.send(topicOut, curDto.getRequestId(), json).get();

            log.info("success produce in kafka topic: {}, partition: {}, offset: {}",
                    topicOut,
                    res.getRecordMetadata().partition(),
                    res.getRecordMetadata().offset());

            trans.setStatus(FocStatus.SUCCESS)
                    .setRespCode("0")
                    .setRespDesc("Sent successfully to Kafka");
            appAdapterTransRepository.updateStatus(trans);

        } catch (Exception e) {
            log.error("error send in topic  {}", topicOut, e);

            trans.setStatus(FocStatus.ERROR)
                    .setRespCode("500")
                    .setRespDesc(e.getMessage());
            appAdapterTransRepository.updateStatus(trans);
            return true;
        }

        AppAdapterIoMsgs ioMsgs = new AppAdapterIoMsgs()
                .setTransId(trans.getId())
                .setMsgType(MsgType.GRU)
                .setDir(Dir.OUT)
                .setMsg(json)
                .setInsTs(LocalDateTime.now())
                .setNodeId(nodeId);
        appAdapterIoMsgsRepository.save(ioMsgs);

        return true;
    }

}
