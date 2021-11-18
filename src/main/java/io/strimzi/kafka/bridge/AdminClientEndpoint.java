/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for admin client endpoint
 */
public abstract class AdminClientEndpoint implements BridgeEndpoint {
    protected final Logger log = LoggerFactory.getLogger(AdminClientEndpoint.class);

    protected String name;
    protected final Vertx vertx;
    protected final BridgeConfig bridgeConfig;

    private Handler<BridgeEndpoint> closeHandler;

    private AdminClient adminClient;

    /**
     * Constructor
     *
     * @param vertx Vert.x instance
     * @param bridgeConfig Bridge configuration
     */
    public AdminClientEndpoint(Vertx vertx, BridgeConfig bridgeConfig) {
        this.vertx = vertx;
        this.name = "kafka-bridge-admin";
        this.bridgeConfig = bridgeConfig;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public BridgeEndpoint closeHandler(Handler<BridgeEndpoint> endpointCloseHandler) {
        this.closeHandler = endpointCloseHandler;
        return this;
    }

    @Override
    public void open() {
        // create an admin client
        KafkaConfig kafkaConfig = this.bridgeConfig.getKafkaConfig();
        Properties props = new Properties();
        props.putAll(kafkaConfig.getConfig());
        props.putAll(kafkaConfig.getAdminConfig().getConfig());

        this.adminClient = AdminClient.create(props);
    }

    @Override
    public void close() {
        if (this.adminClient != null) {
            this.adminClient.close();
        }
        this.handleClose();
    }

    /**
     * Returns all the topics.
     */
    protected void listTopics(Handler<AsyncResult<Set<String>>> handler) {
        log.info("List topics");
        KafkaToVertx.handle(adminClient.listTopics().names(), handler);
    }

    /**
     * Returns the description of the specified topics.
     */
    protected void describeTopics(List<String> topicNames, Handler<AsyncResult<Map<String, TopicDescription>>> handler) {
        log.info("Describe topics {}", topicNames);
        KafkaToVertx.handle(adminClient.describeTopics(topicNames).all(), handler);
    }

    /**
     * Returns the configuration of the specified resources.
     */
    protected void describeConfigs(List<ConfigResource> configResources, Handler<AsyncResult<Map<ConfigResource, Config>>> handler) {
        log.info("Describe configs {}", configResources);
        KafkaToVertx.handle(adminClient.describeConfigs(configResources).all(), handler);
    }

    /**
     * Returns the offset spec for the given partition.
     */
    protected void listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, Handler<AsyncResult<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>>> handler) {
        log.info("Get the offset spec for partition {}", topicPartitionOffsets);
        KafkaToVertx.handle(adminClient.listOffsets(topicPartitionOffsets).all(), handler);
    }

    /**
     * Raise close event
     */
    protected void handleClose() {

        if (this.closeHandler != null) {
            this.closeHandler.handle(this);
        }
    }
}
