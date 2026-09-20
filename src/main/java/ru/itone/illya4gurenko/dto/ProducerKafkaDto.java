package ru.itone.illya4gurenko.dto;

import lombok.Data;
import ru.itone.illya4gurenko.entity.enums.EventType;
import ru.itone.illya4gurenko.entity.enums.MsgType;

import java.util.List;

@Data
public class ProducerKafkaDto {
    private Long actualTimestamp;
    private MsgType systemId;
    private String reqeustId;
    private EventType eventType;
    private List<EventDto> events;

}
