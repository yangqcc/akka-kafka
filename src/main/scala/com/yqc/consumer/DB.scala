package com.yqc.consumer

import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.kafka.ConsumerSettings
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.Future

/**
  *
  */
class DB {
  private val offset = new AtomicLong

  //ªÒ»°≈‰÷√
  private val config: Config = ConfigFactory.load().getConfig("akka.kafka.consumer")

  //¥˙¬Î±‡–¥≈‰÷√
  private val consumerSettings = ConsumerSettings(config, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def save(record: ConsumerRecord[Array[Byte], String]): Future[Done] = {
    println(s"DB.save: ${record.value}")
    offset.set(record.offset)
    Future.successful(Done)
  }

  def loadOffset(): Future[Long] = Future.successful(offset.get)

  def update(data: String): Future[Done] = {
    println(s"DB.update: $data")
    Future.successful(Done)
  }
}
