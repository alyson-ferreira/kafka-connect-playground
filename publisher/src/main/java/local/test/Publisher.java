package local.test;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import local.test.avro.Test;

import org.apache.kafka.common.errors.SerializationException;

import java.util.Date;
import java.util.Properties;
import java.io.IOException;

public class Publisher {

    private static final String TOPIC = System.getenv("KAFKA_TOPIC");
    private static final Properties props = new Properties();

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        Date now = new Date();

        try (KafkaProducer<String, Test> producer = new KafkaProducer<>(props)) {
            final Test included = Test.newBuilder().setInclude(999).build();
            final Test excluded = Test.newBuilder().setExclude("Something").build();
            final ProducerRecord<String, Test> includedRec = new ProducerRecord<>(TOPIC, "+" + now.toString(), included);
            final ProducerRecord<String, Test> excludedRec = new ProducerRecord<>(TOPIC, "-" + now.toString(), excluded);
            final ProducerRecord<String, Test> emptyRec = new ProducerRecord<>(TOPIC, "!" + now.toString(), null);
            producer.send(includedRec);
            producer.send(excludedRec);
            producer.send(emptyRec);
            producer.flush();
        } catch (final SerializationException e) {
            e.printStackTrace();
        }
    }
}
