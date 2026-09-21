package ru.itone.illya4gurenko.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;

import java.util.List;

@Mapper
public interface GruVistaTabRepository {

    void save(GruVistaTab vistaTab);

    List<GruVistaTab> findAll();

    List<GruVistaTab> fetchBatchForUpdate(@Param("limit") int limit);

    void updateStatusByIds(@Param("ids") List<Long> ids, @Param("newStatus") FocStatus newStatus);
}
