package ru.itone.illya4gurenko.dto;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ErrorDto {
    private String code;
    private String message;
}
