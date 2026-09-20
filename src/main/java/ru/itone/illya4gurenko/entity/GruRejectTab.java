package ru.itone.illya4gurenko.entity;

import lombok.Data;
import lombok.experimental.Accessors;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Accessors(chain = true)
public class GruRejectTab {

    private Long id;

    private String systemAccount;

    private Long vistaTanId;

    private Long uterrario;

    private BigDecimal oldTBal;

    private BigDecimal newTBal;

    private LocalDateTime frontSysTimestamp;

    private String frontStatus;

    private String rejectDesc;

    private String checkStatus;

    private String checkDesc;

    private String checkUser;

    private LocalDateTime checkTimestamp;

    private String svfeLoadId;

}
