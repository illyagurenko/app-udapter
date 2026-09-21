package ru.itone.illya4gurenko.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;

import java.util.List;

@Mapper
public interface AppAdapterTransRepository {

    void save(AppAdapterTrans trans);

    List<AppAdapterTrans> findAll();

    void updateStatus(AppAdapterTrans trans);

    AppAdapterTrans findByRequestId(@Param("requestId") String requestId);
}
