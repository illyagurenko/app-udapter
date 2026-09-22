package ru.itone.illya4gurenko.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.repository.AppAdapterIoMsgsRepository;

@Component
@RequiredArgsConstructor
public class AppAdapterIoMsgsDao {
    private final AppAdapterIoMsgsRepository repository;

    public void save(AppAdapterIoMsgs ioMsgs) {
        repository.save(ioMsgs);
    }
}