package ru.itone.illya4gurenko.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.repository.AppAdapterTransRepository;

@Component
@RequiredArgsConstructor
public class AppAdapterTransDao {
    private final AppAdapterTransRepository repository;

    public void save(AppAdapterTrans trans) {
        repository.save(trans);
    }

    public void updateStatus(AppAdapterTrans trans) {
        repository.updateStatus(trans);
    }

    public AppAdapterTrans findByRequestId(String requestId) {
        return repository.findByRequestId(requestId);
    }
}