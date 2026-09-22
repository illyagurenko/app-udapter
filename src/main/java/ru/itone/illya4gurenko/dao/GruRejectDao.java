package ru.itone.illya4gurenko.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.entity.GruRejectTab;
import ru.itone.illya4gurenko.repository.GruRejectTabRepository;

@Component
@RequiredArgsConstructor
public class GruRejectDao {
    private final GruRejectTabRepository repository;

    public void save(GruRejectTab rejectTab) {
        repository.save(rejectTab);
    }
}