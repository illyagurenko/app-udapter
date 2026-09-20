package ru.itone.illya4gurenko.dto;

import lombok.Data;
import ru.itone.illya4gurenko.entity.enums.EntityType;
import ru.itone.illya4gurenko.entity.enums.EventType;
import ru.itone.illya4gurenko.entity.enums.MsgType;

import java.util.List;

@Data
public class ConsumerKafkaDto {
    private Long actualTimestamp;
    private MsgType systemId;
    private String reqeustId;
    private EventType eventType;
    private EntityType entityType;
    private List<EventDto> events;

}
