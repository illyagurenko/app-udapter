package ru.itone.illya4gurenko.repository;

import org.apache.ibatis.annotations.Mapper;
import ru.itone.illya4gurenko.entity.GruRejectTab;

import java.util.List;

@Mapper
public interface GruRejectTabRepository {

    void save(GruRejectTab rejectTab);

    List<GruRejectTab> findAll();
}
