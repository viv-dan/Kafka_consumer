package com.example.consumer.Consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import lombok.extern.log4j.Log4j2;

@Component
@Log4j2
public class LibraryEventsConsumer implements AcknowledgingMessageListener<Integer, String> {

  @Override
  @KafkaListener(topics={"library-events"})
  public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {
    log.info("Consumer record : {}", data);
    acknowledgment.acknowledge();

  }
}
