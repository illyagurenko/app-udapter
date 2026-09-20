package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class GruVistaTabService {

    @Value("${batch.size:100}")
    private int batchSize;

    private final BatchingService batchingService;

    public void process(){
        //приходит дтошка с соткой пушим в таблицу транзакции + добавить сфедулера

    }
}
