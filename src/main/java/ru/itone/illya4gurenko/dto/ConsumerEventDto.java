package ru.itone.illya4gurenko.dto;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ConsumerEventDto {
    private String entityValue;
    private Long entityId;
    private EventDataDto data;
    private String status;
    private ErrorDto error;
}
