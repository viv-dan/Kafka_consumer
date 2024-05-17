package com.example.consumer.Service;

import com.example.consumer.Entity.LibraryEvent;
import com.example.consumer.Repository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Log4j2
public class LibraryService {

  @Autowired
  ObjectMapper objectMapper;

  @Autowired
  KafkaTemplate<Integer,String> kafkaTemplate;

  @Autowired
  private Repository libraryEventsRepository;

  public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
    LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
    log.info("libraryEvent : {} ", libraryEvent);

    if(libraryEvent.getLibraryEventId()!=null && ( libraryEvent.getLibraryEventId()==999 )){
      throw new RecoverableDataAccessException("Temporary Network Issue");
    }

    switch(libraryEvent.getLibraryEventType()){
      case NEW:
        save(libraryEvent);
        break;
      case UPDATE:
        //validate the libraryevent
        validate(libraryEvent);
        save(libraryEvent);
        break;
      default:
        log.info("Invalid Library Event Type");
    }

  }

  private void validate(LibraryEvent libraryEvent) {
    if(libraryEvent.getLibraryEventId()==null){
      throw new IllegalArgumentException("Library Event Id is missing");
    }

    Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
    if(!libraryEventOptional.isPresent()){
      throw new IllegalArgumentException("Not a valid library Event");
    }
    log.info("Validation is successful for the library Event : {} ", libraryEventOptional.get());
  }

  private void save(LibraryEvent libraryEvent) {
    libraryEvent.getBook().setLibraryEvent(libraryEvent);
    libraryEventsRepository.save(libraryEvent);
    log.info("Successfully Persisted the libary Event {} ", libraryEvent);
  }

  public void handleRecovery(ConsumerRecord<Integer,String> record){

    Integer key = record.key();
    String message = record.value();

    var listenableFuture = kafkaTemplate.sendDefault(key, message);
    listenableFuture.whenComplete((sendResult, throwable) -> {
      if (throwable != null) {
        handleFailure(key, message, throwable);
      } else {
        handleSuccess(key, message, sendResult);

      }
    });
  }

  private void handleFailure(Integer key, String value, Throwable ex) {
    log.error("Error Sending the Message and the exception is {}", ex.getMessage());
    try {
      throw ex;
    } catch (Throwable throwable) {
      log.error("Error in OnFailure: {}", throwable.getMessage());
    }
  }

  private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
    log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
  }
}