package ru.itone.illya4gurenko.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import ru.itone.illya4gurenko.entity.enums.EventType;
import ru.itone.illya4gurenko.entity.enums.MsgType;

import java.time.LocalDateTime;

@Data
@Accessors(chain = true)
public class AppAdapterTrans {

    private Long id;

    private MsgType systemId;

    private Long requestId;

    private EventType eventType;

    private String data;

    private String status;

    private String respCode;

    private String respDesc;

    private LocalDateTime insTs;

    private LocalDateTime updTs;
}
