package com.example.consumer.Consumer;


import com.example.consumer.Service.LibraryService;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.stereotype.Component;

import lombok.extern.log4j.Log4j2;

@Component
@Log4j2
public class LibraryEventsConsumer {

  @Autowired
  private LibraryService libraryEventsService;

  @KafkaListener(
          topics = {"library-events"}
          , autoStartup = "${libraryListener.startup:true}"
          , groupId = "library-events-listener-group")
  public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {

    log.info("ConsumerRecord : {} ", consumerRecord);
    libraryEventsService.processLibraryEvent(consumerRecord);

  }
}
