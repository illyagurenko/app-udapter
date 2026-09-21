package ru.itone.illya4gurenko.repository;

import org.apache.ibatis.annotations.Mapper;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;

import java.util.List;

@Mapper
public interface AppAdapterTransRepository {

    void save(AppAdapterTrans trans);

    List<AppAdapterTrans> findAll();
}
