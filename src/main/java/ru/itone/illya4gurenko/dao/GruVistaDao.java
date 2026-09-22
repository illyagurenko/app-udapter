package ru.itone.illya4gurenko.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.repository.GruVistaTabRepository;

import java.math.BigDecimal;
import java.util.List;

@Component
@RequiredArgsConstructor
public class GruVistaDao {
    private final GruVistaTabRepository repository;

    public List<GruVistaTab> fetchBatchForUpdate(int limit) {
        return repository.fetchBatchForUpdate(limit);
    }

    public void updateStatusByIds(List<Long> ids, FocStatus newStatus) {
        repository.updateStatusByIds(ids, newStatus);
    }

    public void updateBalanceAndStatus(Long id, BigDecimal oldTBal, BigDecimal newTBal, FocStatus status) {
        repository.updateBalanceAndStatus(id, oldTBal, newTBal, status);
    }
}