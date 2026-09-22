package ru.itone.illya4gurenko.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;

@Component
@RequiredArgsConstructor
@Slf4j
public class MapperUtils {

    private final ObjectMapper objectMapper;

    public String tryMappingToStr(ProducerKafkaDto curDto){
        String json;
        try {
            json = objectMapper.writeValueAsString(curDto);
            log.error("success mapping\n{}", json);
            return json;
        } catch (Exception e) {
            log.error("error mapping", e);
            return "";
        }
    }


    public ConsumerKafkaDto tryMappingToConsumerKafkaDto(String json){
        ConsumerKafkaDto consumerKafkaDto;
        try {
            consumerKafkaDto = objectMapper.readValue(json, ConsumerKafkaDto.class);
            return consumerKafkaDto;
        } catch (Exception e) {
            log.error("error parse json: {}", json, e);
            return null;
        }
    }

    public ProducerKafkaDto tryMappingToProducerKafkaDto(String json){
        ProducerKafkaDto sentDto;
        try {
            sentDto = objectMapper.readValue(json, ProducerKafkaDto.class);
            return sentDto;
        } catch (Exception e) {
            log.error("error parse sent trans.data");
            return null;
        }
    }

}
