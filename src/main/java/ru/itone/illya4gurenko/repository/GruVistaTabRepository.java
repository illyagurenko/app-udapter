package ru.itone.illya4gurenko.repository;

import org.apache.ibatis.annotations.Mapper;
import ru.itone.illya4gurenko.entity.GruVistaTab;

import java.util.List;

@Mapper
public interface GruVistaTabRepository {

    void save(GruVistaTab config);

    List<GruVistaTab> findAll();
}
