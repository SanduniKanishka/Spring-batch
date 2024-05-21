package com.javatechie.spring.batch.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.javatechie.spring.batch.entity.Customer;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

public class StepSkipListener implements SkipListener<Customer, Number> {

    Logger logger = LoggerFactory.getLogger(StepSkipListener.class);
    public void onSkipInReade(Throwable throwable){
        logger.info("A failure on read {}", throwable.getMessage());
    }
    public void onSkipInWrite(Throwable throwable, Number item){
        logger.info("A failure on write {}, {}", throwable.getMessage(), item);
    }

    @SneakyThrows
    public void onSkipInProcess(Customer customer, Throwable throwable){
        logger.info("Item {} was skipped \" +\n" +
                "                \"due to the exception {} ", new ObjectMapper().writeValueAsString(customer)
                , throwable.getMessage());
    }
}
