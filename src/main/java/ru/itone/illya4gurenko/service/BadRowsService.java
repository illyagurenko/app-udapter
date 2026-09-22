package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.dao.GruVistaDao;

@Service
@RequiredArgsConstructor
@Slf4j
public class BadRowsService {

    private final GruVistaDao gruVistaDao;

    @Scheduled(fixedDelay = 900_000)
    public void recoverStuckRecords() {
        log.info("Starting recovery job for stuck IN_PROCESS records...");
        try {
            gruVistaDao.resetStuckRecords(30);
            log.info("Recovery job finished successfully.");
        } catch (Exception e) {
            log.error("Error during recovery job", e);
        }
    }
}