package ru.itone.illya4gurenko.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.enums.Dir;
import ru.itone.illya4gurenko.entity.enums.MsgType;
import ru.itone.illya4gurenko.repository.AppAdapterIoMsgsRepository;
import ru.itone.illya4gurenko.repository.AppAdapterTransRepository;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerService {
//1. нужно распарсить строку и преобразовать в DTO
//2. сходить существует ли такая сущность в ACC_APP_ADAPTER.APP_ADAPTER_TRANS
//3. зафиксировать полученное json сообщение в IO MSG


//4. проверить все ли записи ENTITY_ID были в патчке если не все то все записи  считаются ERROR
//4. Если вся пачка помечена как ERROR то все записи в GRU.GRU_VISTA_TAB нужно пометить как ERROR
//и все записи должны попасть в GRU_REJECT_TAB
//5.Если есть успешные то мы должны изменить изменить СТАТУС на SUCCESS и изменить NEWTBAL и OLDTBAL
//и поставить запись в SUCCESS

    @Value("${pc.number}")
    private Long numberPC;

    private final ObjectMapper objectMapper;
    private final AppAdapterTransRepository transRepository;
    private final AppAdapterIoMsgsRepository ioMsgsRepository;

    private final AppAdapterIoMsgsRepository appAdapterIoMsgsRepository;

    public void consume(ConsumerRecord<String, String> record){
        String requestId = record.key();
        AppAdapterTrans curTrans = transRepository.findByRequestId(requestId);
        if (curTrans == null) {
            log.warn("trans with requestId={} not found", requestId);
            return;
        }
        String json = record.value();
        try {
            ConsumerKafkaDto consumerKafkaDto = objectMapper.readValue(json, ConsumerKafkaDto.class);
            AppAdapterIoMsgs ioMsgs = new AppAdapterIoMsgs()
                    .setTransId(curTrans.getId())
                    .setMsgType(MsgType.GRU)
                    .setDir(Dir.IN)
                    .setMsg(json)
                    .setInsTs(LocalDateTime.now())
                    .setNodeId(numberPC);
            appAdapterIoMsgsRepository.save(ioMsgs);
        } catch (Exception e) {
            log.error("error mapping consume", e);
        }
    }
}
