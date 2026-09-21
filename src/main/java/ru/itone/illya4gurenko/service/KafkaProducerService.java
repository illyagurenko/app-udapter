package ru.itone.illya4gurenko.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.enums.Dir;
import ru.itone.illya4gurenko.entity.enums.EventType;
import ru.itone.illya4gurenko.entity.enums.MsgType;
import ru.itone.illya4gurenko.entity.enums.TransType;
import ru.itone.illya4gurenko.repository.AppAdapterIoMsgsRepository;
import ru.itone.illya4gurenko.repository.AppAdapterTransRepository;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    @Value("${batch.size}")
    private int batchSize;

    @Value("${pc.number}")
    private Long numberPC;

    private final BatchingService batchingService;
    private final AppAdapterTransRepository appAdapterTransRepository;
    private final AppAdapterIoMsgsRepository appAdapterIoMsgsRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public boolean produce(String topicOut) {
        ConsumerKafkaDto curDto = batchingService.batchAndUpdate(batchSize);
        if (curDto == null || curDto.getEvents() == null || curDto.getEvents().isEmpty()) {
            return false;
        }

        String json;
        try {
            json = objectMapper.writeValueAsString(curDto);
        } catch (Exception e) {
            log.error("error mapping", e);
            return false;
        }

        AppAdapterTrans trans = new AppAdapterTrans()
                .setSystemId(MsgType.GRU)
                .setRequestId(UUID.randomUUID().toString())
                .setEventType(EventType.BALANCE)
                .setData(json)
                .setStatus(TransType.PROGRESS.name())
                .setInsTs(LocalDateTime.now());
        appAdapterTransRepository.save(trans);

        try {
            kafkaTemplate.send(topicOut, curDto.getRequestId(), json).get();

            trans.setStatus(TransType.SUCCESS.name())
                    .setRespCode("0")
                    .setRespDesc("Sent successfully to Kafka");
            appAdapterTransRepository.updateStatus(trans);

        } catch (Exception e) {
            log.error("error send in topic  {}", topicOut, e);

            trans.setStatus(TransType.ERROR.name())
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
                .setNodeId(numberPC);
        appAdapterIoMsgsRepository.save(ioMsgs);

        return true;
    }

}
