package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;

import java.util.HashMap;
import java.util.Map;

public class MainVerticle extends AbstractVerticle {

  String TOPIC_NAME = "my-first-topic";

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    // configure Kafka consumer
    Map<String, String> consumerConfig = new HashMap<>();
    consumerConfig.put("bootstrap.servers", "127.0.0.1:9092");
    consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerConfig.put("group.id", "my_group");
    consumerConfig.put("auto.offset.reset", "earliest");
    consumerConfig.put("enable.auto.commit", "false");

    // instantiate Consumer object
    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerConfig);

    // configure Kafka producer
    Map<String, String> producerConfig = new HashMap<>();
    producerConfig.put("bootstrap.servers", "127.0.0.1:9092");
    producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerConfig.put("acks", "1");

    // instantiate Producer object
    KafkaProducer<String, String> producer = KafkaProducer.create(vertx, producerConfig);

    // register how to handle consumer incoming stream
    consumer.handler(record -> {

      System.out.println("Processing key="+record.key()+",value="+record.value()+
        ",partition="+record.partition()+",offset="+record.offset());
    });

    // set-up routes for vert.x web application
    Router router = Router.router(vertx);

    // POST with no request body - Write 100 strings to a topic
    router.post("/write/:amount").handler(routingContext -> {

      Integer amount = Integer.parseInt(routingContext.request().getParam("amount"));

      if (amount < 0 && amount >= 50000)
        routingContext.response().setStatusCode(400).setStatusMessage("allowed amount must be in N = (0,10000>").end();

      for (int i = 0; i < amount; i++) {
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(TOPIC_NAME, "message_"+i);
        producer.write(record, ar ->{

          if (ar.failed()) {
            System.out.println("Writing failed: "+ar.cause().getMessage());
          } else {
            RecordMetadata recordMetadata = ar.result();
            System.out.println("Message "+record.value()+" written on topic="+recordMetadata.getTopic()+
              ", partition="+recordMetadata.getPartition()+
              ", offset="+recordMetadata.getOffset());
          }
        });
      }
      routingContext.response().setStatusCode(200).end();
    });


    // POST with no request body - subscribe to topic
    router.post("/subscribe").handler(routingContext ->
      consumer.subscribe(TOPIC_NAME, ar -> {

        if (ar.failed()) {
          System.out.println("Could not subscribe "+ar.cause().getMessage());
          routingContext.response().setStatusCode(500).end();
        } else {
          System.out.println("Subscribed");
          routingContext.response().setStatusCode(200).end();
        }
      })
    );

    // POST with no request body - unsubscribe from topic
    router.post("/unsubscribe").handler(routingContext ->
      consumer.unsubscribe(ar -> {

        if (ar.failed()) {
          System.out.println("Could not subscribe "+ar.cause().getMessage());
          routingContext.response().setStatusCode(500).end();
        } else {
          System.out.println("Unsubscribed");
          routingContext.response().setStatusCode(200).end();
        }
      })
    );

    vertx.createHttpServer().requestHandler(router::accept).listen(8080, http -> {

      if (http.succeeded()) {
        startFuture.complete();
        System.out.println("HTTP server started on http://localhost:8080");
      } else {
        startFuture.fail(http.cause());
      }
    });
  }
}

