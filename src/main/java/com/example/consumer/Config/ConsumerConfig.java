package com.example.consumer.Config;

import com.example.consumer.Service.LibraryService;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

import lombok.extern.log4j.Log4j2;

@Configuration
@EnableKafka
@Log4j2
public class ConsumerConfig {

  public static final String RETRY = "RETRY";
  public static final String SUCCESS = "SUCCESS";
  public static final String DEAD = "DEAD";

  @Autowired
  LibraryService libraryEventsService;

  @Autowired
  KafkaProperties kafkaProperties;

  @Autowired
  KafkaTemplate kafkaTemplate;

  @Value("${topics.retry:library-events.RETRY}")
  private String retryTopic;

  @Value("${topics.dlt:library-events.DLT}")
  private String deadLetterTopic;


  public DeadLetterPublishingRecoverer publishingRecoverer() {

    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate
            , (r, e) -> {
      log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
      if (e.getCause() instanceof RecoverableDataAccessException) {
        return new TopicPartition(retryTopic, r.partition());
      } else {
        return new TopicPartition(deadLetterTopic, r.partition());
      }
    }
    );

    return recoverer;

  }


  public DefaultErrorHandler errorHandler() {

    var exceptiopnToIgnorelist = List.of(
            IllegalArgumentException.class
    );

    ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(2);
    expBackOff.setInitialInterval(1_000L);
    expBackOff.setMultiplier(2.0);
    expBackOff.setMaxInterval(2_000L);

    var fixedBackOff = new FixedBackOff(1000L, 2L);

    var defaultErrorHandler = new DefaultErrorHandler(
            publishingRecoverer()
            ,
            fixedBackOff
    );

    exceptiopnToIgnorelist.forEach(defaultErrorHandler::addNotRetryableExceptions);

    defaultErrorHandler.setRetryListeners(
            (record, ex, deliveryAttempt) ->
                    log.info("Failed Record in Retry Listener  exception : {} , deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt)
    );

    return defaultErrorHandler;
  }

  @Bean
  @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
  ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
          ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
          ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
    ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
    configurer.configure(factory, kafkaConsumerFactory
            .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.kafkaProperties.buildConsumerProperties())));
    factory.setConcurrency(3);
    factory.setCommonErrorHandler(errorHandler());
    return factory;
  }

}
