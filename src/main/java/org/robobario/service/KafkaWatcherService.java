package org.robobario.service;

import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class KafkaWatcherService implements Runnable {

    private boolean enabled = true;

    private final KafkaGroupedEvents events;

    private static final byte MAGIC_BYTE = 0x0;

    private static final int idSize = 4;

    private final DecoderFactory decoderFactory = DecoderFactory.get();


    public KafkaWatcherService(KafkaGroupedEvents events) {
        this.events = events;
    }


    public void run() {
        ImmutableMap<String, Object> config = ImmutableMap.of("bootstrap.servers", "localhost:9092");
        CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient("http://localhost:9093", 1000);
        KafkaConsumer<Object, GenericRecord> kafkaConsumer = new KafkaConsumer<Object, GenericRecord>(config, getStringDeserializer(),
            getDeserializer(schemaRegistry));
        consumeEndlessly(kafkaConsumer);
    }


    private void consumeEndlessly(KafkaConsumer<Object, GenericRecord> kafkaConsumer) {
        List<PartitionInfo> partitions = kafkaConsumer.partitionsFor("dsp_event");
        List<TopicPartition> topicPartitions = partitions.stream().map(part -> new TopicPartition(part.topic(), part.partition())).collect(toList());
        kafkaConsumer.assign(topicPartitions);
        while (enabled) {
            ConsumerRecords<Object, GenericRecord> poll = kafkaConsumer.poll(1000);
            for (ConsumerRecord<Object, GenericRecord> record : poll) {
                events.add(record.value());
            }
        }
    }


    public static void main(String[] args) {
        KafkaWatcherService watcherService = new KafkaWatcherService(new KafkaGroupedEvents());
        watcherService.run();
    }


    private Deserializer<Object> getStringDeserializer() {
        return (Deserializer) new StringDeserializer();
    }


    private Deserializer<GenericRecord> getDeserializer(final CachedSchemaRegistryClient schemaRegistry) {
        return new Deserializer<GenericRecord>() {

            public void configure(Map<String, ?> configs, boolean isKey) {

            }


            private ByteBuffer getByteBuffer(byte[] payload) {
                ByteBuffer buffer = ByteBuffer.wrap(payload);
                if (buffer.get() != MAGIC_BYTE) {
                    throw new SerializationException("Unknown magic byte!");
                }
                return buffer;
            }


            public GenericRecord deserialize(String topic, byte[] payload) {
                if (payload == null) {
                    return null;
                }

                int id = -1;
                try {
                    ByteBuffer buffer = getByteBuffer(payload);
                    id = buffer.getInt();
                    Schema schema = schemaRegistry.getByID(id);
                    int length = buffer.limit() - 1 - idSize;
                    int start = buffer.position() + buffer.arrayOffset();
                    DatumReader reader = new GenericDatumReader(schema);
                    return (GenericRecord) reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));
                }
                catch (IOException e) {
                    throw new SerializationException("Error deserializing Avro message for id " + id, e);
                }
                catch (RestClientException e) {
                    throw new SerializationException("Error retrieving Avro schema for id " + id, e);
                }
                catch (RuntimeException e) {
                    // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
                    throw new SerializationException("Error deserializing Avro message for id " + id, e);
                }
            }


            public void close() {
            }
        };
    }

}
