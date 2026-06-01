package com.netflix;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.avro.AvroDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.netflix.ads.AdEvent;

public class AdEventConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        KafkaSource<AdEvent> source = KafkaSource.<AdEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("ad_events_avro")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(AvroDeserializationSchema.forSpecific(AdEvent.class))
                .build();

        DataStream<AdEvent> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        stream.map(event -> {
            System.out.println("📺 Campaign: " + event.getCampaignId() +
                    ", Country: " + event.getCountryCode());
            return event;
        }).print();

        env.execute("Avro Ad Event Consumer");
    }
}