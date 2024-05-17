package com.example.consumer;

import com.example.consumer.Entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface Repository extends CrudRepository<LibraryEvent,Integer> {
}