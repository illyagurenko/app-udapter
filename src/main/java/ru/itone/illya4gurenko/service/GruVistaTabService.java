package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.entity.AppAdapterConfig;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.enums.Dir;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.entity.enums.MsgType;
import ru.itone.illya4gurenko.entity.enums.TransType;
import ru.itone.illya4gurenko.repository.AppAdapterIoMsgsRepository;
import ru.itone.illya4gurenko.repository.AppAdapterTransRepository;
import ru.itone.illya4gurenko.сonfig.AppDatabaseInitializer;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class GruVistaTabService {

    @Value("${batch.size}")
    private int batchSize;

    @Value("${pc.number}")
    private Long numberPC;

    private final BatchingService batchingService;
    private final AppAdapterTransRepository appAdapterTransRepository;
    private final AppAdapterIoMsgsRepository appAdapterIoMsgsRepository;
    private final AppDatabaseInitializer initializer;

    @Scheduled(fixedDelayString = "${batch.timeout.worker.delay}", timeUnit = TimeUnit.SECONDS)
    public void sendData(int batchSize){
        //приходит дтошка с соткой пушим в таблицу транзакции + добавить сфедулера
        while(true){
            ConsumerKafkaDto curDto = batchingService.batchAndUpdate(batchSize);
            if(curDto == null || curDto.getEvents() == null || curDto.getEvents().isEmpty()){
                log.debug("empty data");
                break;
            }
            AppAdapterConfig appAdapterConfig = initializer.getAppAdapterConfig();
            AppAdapterTrans appAdapterTrans = new AppAdapterTrans()
                    .setSystemId(appAdapterConfig.getSystemId())
                    .setRequestId(1L)
                    .setEventType(appAdapterConfig.getEventType())
                    .setData(curDto.toString())
                    .setStatus(TransType.SEND_TO_KAFKA.name())
                    .setRespCode(null)
                    .setRespDesc(null)
                    .setInsTs(LocalDateTime.now())
                    .setUpdTs(null);

            appAdapterTransRepository.save(appAdapterTrans);
            // в кафку

            AppAdapterIoMsgs appAdapterIoMsgs = new AppAdapterIoMsgs()
                    .setTransId(appAdapterTrans.getId())
                    .setMsgType(MsgType.GRU)
                    .setDir(Dir.IN)
                    .setMsg(appAdapterTrans.getData())
                    .setInsTs(LocalDateTime.now())
                    .setNodeId(numberPC);

            appAdapterIoMsgsRepository.save(appAdapterIoMsgs);
        }
    }

}
