package ru.itone.dto;

import ru.itone.illya4gurenko.entity.enums.Type;

import java.math.BigDecimal;

public class EventDto {

    private Long id;

    private String systemAccount;

    private String currency;

    private BigDecimal xalfa;

    private Type operation;

}
