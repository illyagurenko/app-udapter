package ru.itone.illya4gurenko.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import ru.itone.illya4gurenko.entity.enums.Dir;
import ru.itone.illya4gurenko.entity.enums.MsgType;

import java.time.LocalDateTime;

@Data
@Accessors(chain = true)
public class AppAdapterIoMsgs {

    private Long id;

    private Long transId;

    private MsgType msgType;

    private Dir dir;

    private String msg;

    private LocalDateTime insTs;

    private Long nodeId;

}
