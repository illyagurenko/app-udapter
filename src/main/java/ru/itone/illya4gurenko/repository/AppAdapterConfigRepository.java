package ru.itone.illya4gurenko.repository;


import org.apache.ibatis.annotations.Mapper;
import ru.itone.illya4gurenko.entity.AppAdapterConfig;

import java.util.List;

@Mapper
public interface AppAdapterConfigRepository {

    void save(AppAdapterConfig config);

    List<AppAdapterConfig> findAll();

}
