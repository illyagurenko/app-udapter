package ru.itone.illya4gurenko.repository;

import org.apache.ibatis.annotations.Mapper;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;

import java.util.List;

@Mapper
public interface AppAdapterIoMsgsRepository {

    void save(AppAdapterIoMsgs config);

    List<AppAdapterIoMsgs> findAll();
}
