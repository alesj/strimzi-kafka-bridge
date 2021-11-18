/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.amqp.converter;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.strimzi.kafka.bridge.amqp.AmqpBridge;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;

/**
 * Raw implementation class for the message conversion
 * between Kafka record and AMQP message.
 * It passes the AMQP message as is (raw bytes) as Kafka record value and vice versa.
 */
public class AmqpRawMessageConverter implements MessageConverter<String, byte[], Message, Collection<Message>> {

    // TODO : should be it configurable or based on max frame size ?
    private static final int BUFFER_SIZE = 32768;

    @Override
    public ProducerRecord<String, byte[]> toKafkaRecord(String kafkaTopic, Integer partition, Message message) {

        Object partitionFromMessage = null, key = null;
        byte[] value;
        byte[] buffer = new byte[AmqpRawMessageConverter.BUFFER_SIZE];

        // get topic and body from AMQP message
        String topic = (message.getAddress() == null) ?
                kafkaTopic :
                message.getAddress().replace('/', '.');

        int encoded = message.encode(buffer, 0, AmqpRawMessageConverter.BUFFER_SIZE);
        value = Arrays.copyOfRange(buffer, 0, encoded);

        // get partition and key from AMQP message annotations
        // NOTE : they are not mandatory
        MessageAnnotations messageAnnotations = message.getMessageAnnotations();

        if (messageAnnotations != null) {

            partitionFromMessage = messageAnnotations.getValue().get(Symbol.getSymbol(AmqpBridge.AMQP_PARTITION_ANNOTATION));
            key = messageAnnotations.getValue().get(Symbol.getSymbol(AmqpBridge.AMQP_KEY_ANNOTATION));

            if (partitionFromMessage != null && !(partitionFromMessage instanceof Integer))
                throw new IllegalArgumentException("The partition annotation must be an Integer");

            if (key != null && !(key instanceof String))
                throw new IllegalArgumentException("The key annotation must be a String");
        }

        // build the record for the KafkaProducer and then send it
        return new ProducerRecord<>(topic, (Integer) partitionFromMessage, (String) key, value);
    }

    @Override
    public Message toMessage(String address, ConsumerRecord<String, byte[]> record) {

        Message message = Proton.message();
        message.setAddress(address);

        message.decode(record.value(), 0, record.value().length);

        // put message annotations about partition, offset and key (if not null)
        Map<Symbol, Object> map = new HashMap<>();
        map.put(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_ANNOTATION), record.partition());
        map.put(Symbol.valueOf(AmqpBridge.AMQP_OFFSET_ANNOTATION), record.offset());
        map.put(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION), record.key());
        map.put(Symbol.valueOf(AmqpBridge.AMQP_TOPIC_ANNOTATION), record.topic());

        MessageAnnotations messageAnnotations = new MessageAnnotations(map);
        message.setMessageAnnotations(messageAnnotations);

        return message;
    }

    @Override
    public Collection<Message> toMessages(ConsumerRecords<String, byte[]> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ProducerRecord<String, byte[]>> toKafkaRecords(String kafkaTopic, Integer partition, Collection<Message> messages) {
        throw new UnsupportedOperationException();
    }
}
