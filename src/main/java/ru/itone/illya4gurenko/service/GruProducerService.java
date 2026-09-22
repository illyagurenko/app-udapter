package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.dao.AppAdapterIoMsgsDao;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;
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
public class GruProducerService {

    @Value("${batch.size}")
    private int batchSize;

    @Value("${pc.number}")
    private Long nodeId;

    private final BatchingService batchingService;
    private final AppAdapterTransDao transDao;
    private final AppAdapterIoMsgsDao ioMsgsDao;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MapperUtils mapperUtils;
    private final AdapterEntityFactory adapterEntityFactory;

    public boolean produce(String topicOut) {
        ProducerKafkaDto curDto = batchingService.batchAndUpdate(batchSize);
        if (curDto == null || curDto.getEvents() == null || curDto.getEvents().isEmpty()) {
            log.trace("not rows with status WAIT");
            return false;
        }

        log.info("found batch {} rows requestId={}", curDto.getEvents().size(), curDto.getRequestId());

        String json = mapperUtils.tryMappingToStr(curDto);
        if(json.isEmpty()){
            return false;
        }

        AppAdapterTrans trans = adapterEntityFactory.createTrans(curDto, json);
        transDao.save(trans);

        try {
            SendResult<String, String> res = kafkaTemplate.send(topicOut, curDto.getRequestId(), json).get();

            log.info("success produce in kafka topic: {}, partition: {}, offset: {}",
                    topicOut,
                    res.getRecordMetadata().partition(),
                    res.getRecordMetadata().offset());

            trans.setStatus(FocStatus.SUCCESS)
                    .setRespCode("0")
                    .setRespDesc("Sent successfully to Kafka");
            transDao.updateStatus(trans);

        } catch (Exception e) {
            log.error("error send in topic  {}", topicOut, e);

            trans.setStatus(FocStatus.ERROR)
                    .setRespCode("500")
                    .setRespDesc(e.getMessage());
            transDao.updateStatus(trans);
            return true;
        }

        AppAdapterIoMsgs ioMsgs = adapterEntityFactory.createIoMsg(trans.getId(), Dir.OUT, json, nodeId);
        ioMsgsDao.save(ioMsgs);

        return true;
    }

}
