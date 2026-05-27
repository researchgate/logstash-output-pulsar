package org.apache.pulsar.logstash.outputs;

import co.elastic.logstash.api.Codec;
import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.Output;
import co.elastic.logstash.api.PluginConfigSpec;
import co.elastic.logstash.api.PluginHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@LogstashPlugin(name = "pulsar")
public class Pulsar implements Output {

    public static final PluginConfigSpec<Codec> CONFIG_CODEC =
            PluginConfigSpec.codecSetting("codec", "java_line");

    private static final Logger logger = LogManager.getLogger(Pulsar.class);

    private static final PluginConfigSpec<String> CONFIG_SERVICE_URL =
            PluginConfigSpec.stringSetting("serviceUrl", "pulsar://localhost:6650");

    private static final PluginConfigSpec<String> CONFIG_TOPIC =
            PluginConfigSpec.requiredStringSetting("topic");

    private static final String COMPRESSION_TYPE_NONE = "NONE";
    private static final String COMPRESSION_TYPE_LZ4 = "LZ4";
    private static final String COMPRESSION_TYPE_ZLIB = "ZLIB";
    private static final String COMPRESSION_TYPE_ZSTD = "ZSTD";
    private static final String COMPRESSION_TYPE_SNAPPY = "SNAPPY";
    private static final PluginConfigSpec<String> CONFIG_COMPRESSION_TYPE =
            PluginConfigSpec.stringSetting("compression_type", COMPRESSION_TYPE_NONE);

    private static final PluginConfigSpec<Boolean> CONFIG_BLOCK_IF_QUEUE_FULL =
            PluginConfigSpec.booleanSetting("block_if_queue_full", true);

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_BATCHING =
            PluginConfigSpec.booleanSetting("enable_batching", true);

    private static final PluginConfigSpec<String> CONFIG_SEND_TIMEOUT_MS =
            PluginConfigSpec.stringSetting("send_timeout_ms", "30000");

    private static final PluginConfigSpec<String> CONFIG_MAX_PENDING_MESSAGES =
            PluginConfigSpec.stringSetting("max_pending_messages", "1000");

    // Retry configuration
    private static final PluginConfigSpec<String> CONFIG_MAX_RETRIES =
            PluginConfigSpec.stringSetting("max_retries", "3");

    private static final PluginConfigSpec<String> CONFIG_RETRY_INITIAL_DELAY_MS =
            PluginConfigSpec.stringSetting("retry_initial_delay_ms", "100");

    private static final PluginConfigSpec<String> CONFIG_RETRY_MAX_DELAY_MS =
            PluginConfigSpec.stringSetting("retry_max_delay_ms", "10000");

    // Drain timeout on shutdown
    private static final PluginConfigSpec<String> CONFIG_DRAIN_TIMEOUT_MS =
            PluginConfigSpec.stringSetting("drain_timeout_ms", "30000");

    // TLS Config
    private static final String DEFAULT_AUTH_PLUGIN_CLASS_NAME = "org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls";
    private static final List<String> DEFAULT_PROTOCOLS = Arrays.asList("TLSv1.2");
    private static final List<String> DEFAULT_CIPHERS = Arrays.asList(
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"
    );

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_TLS =
            PluginConfigSpec.booleanSetting("enable_tls", false);

    private static final PluginConfigSpec<Boolean> CONFIG_ALLOW_TLS_INSECURE_CONNECTION =
            PluginConfigSpec.booleanSetting("allow_tls_insecure_connection", false);

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_TLS_HOSTNAME_VERIFICATION =
            PluginConfigSpec.booleanSetting("enable_tls_hostname_verification", false);

    private static final PluginConfigSpec<String> CONFIG_TLS_TRUST_STORE_PATH =
            PluginConfigSpec.stringSetting("tls_trust_store_path", "");

    private static final PluginConfigSpec<String> CONFIG_TLS_TRUST_STORE_PASSWORD =
            PluginConfigSpec.stringSetting("tls_trust_store_password", "");

    private static final PluginConfigSpec<String> CONFIG_AUTH_PLUGIN_CLASS_NAME =
            PluginConfigSpec.stringSetting("auth_plugin_class_name", DEFAULT_AUTH_PLUGIN_CLASS_NAME);

    private static final PluginConfigSpec<List<Object>> CONFIG_CIPHERS =
            PluginConfigSpec.arraySetting("ciphers", Collections.singletonList(DEFAULT_CIPHERS), false, false);

    private static final PluginConfigSpec<List<Object>> CONFIG_PROTOCOLS =
            PluginConfigSpec.arraySetting("protocols", Collections.singletonList(DEFAULT_PROTOCOLS), false, false);

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_TOKEN =
            PluginConfigSpec.booleanSetting("enable_token", false);

    private static final PluginConfigSpec<String> CONFIG_AUTH_PLUGIN_PARAMS_STRING =
            PluginConfigSpec.stringSetting("auth_plugin_params_string", "");

    private static final PluginConfigSpec<String> CONFIG_PRODUCER_NAME =
            PluginConfigSpec.stringSetting("producer_name", "logstash-pulsar");

    private final CountDownLatch done = new CountDownLatch(1);

    private final String producerName;
    private final String id;
    private volatile boolean stopped;
    private final PulsarClient client;
    private final ConcurrentHashMap<String, org.apache.pulsar.client.api.Producer<byte[]>> producerMap;
    private final String serviceUrl;
    private final Codec codec;
    private final String topic;
    private final String compressionType;
    private final boolean blockIfQueueFull;
    private final boolean enableBatching;
    private final long sendTimeoutMs;
    private final int maxPendingMessages;
    private final boolean enableTls;
    private final boolean enableToken;

    // Retry settings
    private final int maxRetries;
    private final long retryInitialDelayMs;
    private final long retryMaxDelayMs;
    private final long drainTimeoutMs;

    // Metrics
    private final AtomicLong messagesSent = new AtomicLong(0);
    private final AtomicLong messagesFailed = new AtomicLong(0);
    private final AtomicLong messagesRetried = new AtomicLong(0);
    private final AtomicLong inFlightMessages = new AtomicLong(0);

    // all plugins must provide a constructor that accepts id, Configuration, and Context
    public Pulsar(final String id, final Configuration configuration, final Context context) {
        this.id = id;
        codec = configuration.get(CONFIG_CODEC);
        if (codec == null) {
            throw new IllegalStateException("Unable to obtain codec");
        }

        serviceUrl = configuration.get(CONFIG_SERVICE_URL);
        topic = configuration.get(CONFIG_TOPIC);
        producerName = configuration.get(CONFIG_PRODUCER_NAME);
        enableBatching = configuration.get(CONFIG_ENABLE_BATCHING);
        blockIfQueueFull = configuration.get(CONFIG_BLOCK_IF_QUEUE_FULL);
        compressionType = configuration.get(CONFIG_COMPRESSION_TYPE);
        sendTimeoutMs = Long.parseLong(configuration.get(CONFIG_SEND_TIMEOUT_MS));
        maxPendingMessages = Integer.parseInt(configuration.get(CONFIG_MAX_PENDING_MESSAGES));

        // Retry config
        maxRetries = Integer.parseInt(configuration.get(CONFIG_MAX_RETRIES));
        retryInitialDelayMs = Long.parseLong(configuration.get(CONFIG_RETRY_INITIAL_DELAY_MS));
        retryMaxDelayMs = Long.parseLong(configuration.get(CONFIG_RETRY_MAX_DELAY_MS));
        drainTimeoutMs = Long.parseLong(configuration.get(CONFIG_DRAIN_TIMEOUT_MS));

        enableTls = configuration.get(CONFIG_ENABLE_TLS);
        enableToken = configuration.get(CONFIG_ENABLE_TOKEN);

        try {
            if (enableTls && enableToken) {
                logger.error("Cannot enable both TLS keystore auth and Token auth simultaneously");
                throw new IllegalStateException("Cannot enable both enable_tls and enable_token at the same time");
            } else if (enableTls) {
                client = buildTlsPulsar(configuration);
            } else if (enableToken) {
                client = buildTokenPulsar(configuration);
            } else {
                client = buildNotTlsPulsar();
            }
            producerMap = new ConcurrentHashMap<>();
        } catch (PulsarClientException e) {
            logger.error("Failed to create Pulsar client at {}", serviceUrl, e);
            throw new IllegalStateException("Unable to create Pulsar client: " + e.getMessage(), e);
        }

        logger.info("Pulsar output plugin initialized: serviceUrl={}, topic={}, enableBatching={}, " +
                "blockIfQueueFull={}, compressionType={}, sendTimeoutMs={}, maxRetries={}, " +
                "retryInitialDelayMs={}, drainTimeoutMs={}",
                serviceUrl, topic, enableBatching, blockIfQueueFull, compressionType,
                sendTimeoutMs, maxRetries, retryInitialDelayMs, drainTimeoutMs);
    }

    private PulsarClient buildNotTlsPulsar() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();
    }

    private PulsarClient buildTokenPulsar(Configuration configuration) throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .authentication(
                        configuration.get(CONFIG_AUTH_PLUGIN_CLASS_NAME),
                        configuration.get(CONFIG_AUTH_PLUGIN_PARAMS_STRING))
                .build();
    }

    private PulsarClient buildTlsPulsar(Configuration configuration) throws PulsarClientException {
        Boolean allowTlsInsecureConnection = configuration.get(CONFIG_ALLOW_TLS_INSECURE_CONNECTION);
        Boolean enableTlsHostnameVerification = configuration.get(CONFIG_ENABLE_TLS_HOSTNAME_VERIFICATION);
        String tlsTrustStorePath = configuration.get(CONFIG_TLS_TRUST_STORE_PATH);
        Map<String, String> authMap = new HashMap<>();
        authMap.put(AuthenticationKeyStoreTls.KEYSTORE_TYPE, "JKS");
        authMap.put(AuthenticationKeyStoreTls.KEYSTORE_PATH, tlsTrustStorePath);
        authMap.put(AuthenticationKeyStoreTls.KEYSTORE_PW, configuration.get(CONFIG_TLS_TRUST_STORE_PASSWORD));

        Set<String> cipherSet = new HashSet<>();
        Optional.ofNullable(configuration.get(CONFIG_CIPHERS)).ifPresent(
                cipherList -> cipherList.forEach(cipher -> cipherSet.add(String.valueOf(cipher))));

        Set<String> protocolSet = new HashSet<>();
        Optional.ofNullable(configuration.get(CONFIG_PROTOCOLS)).ifPresent(
                protocolList -> protocolList.forEach(protocol -> protocolSet.add(String.valueOf(protocol))));

        return PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .tlsCiphers(cipherSet)
                .tlsProtocols(protocolSet)
                .allowTlsInsecureConnection(allowTlsInsecureConnection)
                .enableTlsHostnameVerification(enableTlsHostnameVerification)
                .tlsTrustStorePath(tlsTrustStorePath)
                .tlsTrustStorePassword(configuration.get(CONFIG_TLS_TRUST_STORE_PASSWORD))
                .authentication(configuration.get(CONFIG_AUTH_PLUGIN_CLASS_NAME), authMap)
                .build();
    }

    @Override
    public void output(final Collection<Event> events) {
        Iterator<Event> z = events.iterator();
        while (z.hasNext() && !stopped) {
            Event event = z.next();
            try {
                String eventTopic = event.sprintf(topic);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                codec.encode(event, baos);
                byte[] messageBytes = baos.toByteArray();

                if (logger.isDebugEnabled()) {
                    logger.debug("topic={}, message={}", eventTopic,
                            new String(messageBytes, StandardCharsets.UTF_8));
                }

                sendWithRetry(eventTopic, messageBytes, 0);
            } catch (Exception e) {
                messagesFailed.incrementAndGet();
                logger.error("Failed to send message", e);
            }
        }
    }

    /**
     * Sends a message asynchronously with exponential backoff retry on failure.
     */
    private void sendWithRetry(String eventTopic, byte[] messageBytes, int attempt) {
        if (stopped && attempt > 0) {
            // Don't retry if we're shutting down
            messagesFailed.incrementAndGet();
            return;
        }

        try {
            org.apache.pulsar.client.api.Producer<byte[]> producer = getProducer(eventTopic);
            inFlightMessages.incrementAndGet();

            producer.newMessage()
                    .value(messageBytes)
                    .sendAsync()
                    .whenComplete((msgId, ex) -> {
                        inFlightMessages.decrementAndGet();
                        if (ex != null) {
                            handleSendFailure(eventTopic, messageBytes, attempt, ex);
                        } else {
                            messagesSent.incrementAndGet();
                            if (logger.isTraceEnabled()) {
                                logger.trace("Message sent to topic {} with id {}",
                                        eventTopic, msgId);
                            }
                        }
                    });
        } catch (Exception e) {
            // Failed to get producer — retry the whole operation
            handleSendFailure(eventTopic, messageBytes, attempt, e);
        }
    }

    /**
     * Handles a send failure with exponential backoff retry.
     */
    private void handleSendFailure(String eventTopic, byte[] messageBytes, int attempt, Throwable ex) {
        if (attempt < maxRetries && !stopped) {
            long delay = calculateBackoffDelay(attempt);
            messagesRetried.incrementAndGet();
            logger.warn("Send to topic {} failed (attempt {}/{}), retrying in {}ms: {}",
                    eventTopic, attempt + 1, maxRetries, delay, ex.getMessage());

            // Schedule retry with backoff
            CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                if (!stopped) {
                    sendWithRetry(eventTopic, messageBytes, attempt + 1);
                } else {
                    messagesFailed.incrementAndGet();
                }
            });
        } else {
            messagesFailed.incrementAndGet();
            logger.error("Failed to send message to topic {} after {} attempts: {}",
                    eventTopic, attempt + 1, ex.getMessage());
        }
    }

    /**
     * Calculates exponential backoff delay with jitter, capped at retryMaxDelayMs.
     */
    private long calculateBackoffDelay(int attempt) {
        long delay = retryInitialDelayMs * (1L << attempt); // exponential: initial * 2^attempt
        delay = Math.min(delay, retryMaxDelayMs);
        // Add up to 25% jitter to avoid thundering herd
        long jitter = (long) (delay * 0.25 * Math.random());
        return delay + jitter;
    }

    /**
     * Gets or creates a producer for the given topic. If an existing producer is
     * disconnected, it is removed and a new one is created (health check / reconnect).
     */
    private org.apache.pulsar.client.api.Producer<byte[]> getProducer(String topic) throws PulsarClientException {
        // Fast path: check if existing producer is healthy
        org.apache.pulsar.client.api.Producer<byte[]> existing = producerMap.get(topic);
        if (existing != null && existing.isConnected()) {
            return existing;
        }

        // Producer is missing or disconnected — reconnect
        if (existing != null && !existing.isConnected()) {
            logger.warn("Producer for topic {} is disconnected, reconnecting...", topic);
            producerMap.remove(topic, existing);
            // Close the stale producer asynchronously to not block
            CompletableFuture.runAsync(() -> {
                try {
                    existing.close();
                } catch (PulsarClientException e) {
                    logger.debug("Error closing stale producer for topic {}: {}", topic, e.getMessage());
                }
            });
        }

        // Create new producer (computeIfAbsent ensures only one thread creates per topic)
        return producerMap.computeIfAbsent(topic, t -> {
            try {
                return createProducer(t);
            } catch (PulsarClientException e) {
                logger.error("Failed to create producer for topic {}", t, e);
                throw new RuntimeException("Failed to create producer for topic: " + t, e);
            }
        });
    }

    /**
     * Creates a new producer for the given topic.
     */
    private org.apache.pulsar.client.api.Producer<byte[]> createProducer(String topic) throws PulsarClientException {
        ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .topic(topic)
                .enableBatching(enableBatching)
                .blockIfQueueFull(blockIfQueueFull)
                .sendTimeout((int) sendTimeoutMs, TimeUnit.MILLISECONDS)
                .maxPendingMessages(maxPendingMessages)
                .compressionType(getCompressionType());

        if (producerName != null && !producerName.isEmpty()) {
            String uniqProducerName = producerName + '-' + UUID.randomUUID();
            producerBuilder.producerName(uniqProducerName);
        }

        org.apache.pulsar.client.api.Producer<byte[]> newProducer = producerBuilder.create();
        logger.info("Created producer {} for topic {}, blockIfQueueFull={}, compressionType={}",
                newProducer.getProducerName(), topic, blockIfQueueFull, compressionType);
        return newProducer;
    }

    private CompressionType getCompressionType() {
        switch (compressionType.toUpperCase()) {
            case COMPRESSION_TYPE_LZ4:
                return CompressionType.LZ4;
            case COMPRESSION_TYPE_ZLIB:
                return CompressionType.ZLIB;
            case COMPRESSION_TYPE_ZSTD:
                return CompressionType.ZSTD;
            case COMPRESSION_TYPE_SNAPPY:
                return CompressionType.SNAPPY;
            case COMPRESSION_TYPE_NONE:
                return CompressionType.NONE;
            default:
                logger.warn("Unknown compression type '{}', using NONE", compressionType);
                return CompressionType.NONE;
        }
    }

    /**
     * Waits for all in-flight async messages to complete (graceful drain),
     * then closes all producers and the client.
     */
    private void closePulsarProducers() {
        // Graceful drain: wait for in-flight messages to complete
        long drainStart = System.currentTimeMillis();
        long remaining = drainTimeoutMs;
        while (inFlightMessages.get() > 0 && remaining > 0) {
            logger.info("Draining {} in-flight messages ({}ms remaining)...",
                    inFlightMessages.get(), remaining);
            try {
                Thread.sleep(Math.min(500, remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while draining in-flight messages");
                break;
            }
            remaining = drainTimeoutMs - (System.currentTimeMillis() - drainStart);
        }

        if (inFlightMessages.get() > 0) {
            logger.warn("Drain timeout reached with {} messages still in-flight",
                    inFlightMessages.get());
        }

        logger.info("Closing Pulsar producers. Messages sent: {}, failed: {}, retried: {}",
                messagesSent.get(), messagesFailed.get(), messagesRetried.get());

        for (Map.Entry<String, org.apache.pulsar.client.api.Producer<byte[]>> entry : producerMap.entrySet()) {
            org.apache.pulsar.client.api.Producer<byte[]> producer = entry.getValue();
            try {
                producer.flush();
                producer.close();
                logger.info("Closed producer {} for topic {}",
                        producer.getProducerName(), producer.getTopic());
            } catch (PulsarClientException e) {
                logger.error("Error closing producer {} for topic {}",
                        producer.getProducerName(), producer.getTopic(), e);
            }
        }
        producerMap.clear();

        try {
            client.close();
            logger.info("Closed Pulsar client");
        } catch (PulsarClientException e) {
            logger.error("Error closing Pulsar client", e);
        }
    }

    @Override
    public void stop() {
        stopped = true;
        closePulsarProducers();
        done.countDown();
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return PluginHelper.commonOutputSettings(Arrays.asList(
                CONFIG_CODEC,
                CONFIG_SERVICE_URL,
                CONFIG_TOPIC,
                CONFIG_PRODUCER_NAME,
                CONFIG_COMPRESSION_TYPE,
                CONFIG_ENABLE_BATCHING,
                CONFIG_BLOCK_IF_QUEUE_FULL,
                CONFIG_SEND_TIMEOUT_MS,
                CONFIG_MAX_PENDING_MESSAGES,
                CONFIG_MAX_RETRIES,
                CONFIG_RETRY_INITIAL_DELAY_MS,
                CONFIG_RETRY_MAX_DELAY_MS,
                CONFIG_DRAIN_TIMEOUT_MS,
                CONFIG_AUTH_PLUGIN_CLASS_NAME,

                // Pulsar TLS Config
                CONFIG_ENABLE_TLS,
                CONFIG_TLS_TRUST_STORE_PATH,
                CONFIG_TLS_TRUST_STORE_PASSWORD,
                CONFIG_PROTOCOLS,
                CONFIG_ALLOW_TLS_INSECURE_CONNECTION,
                CONFIG_ENABLE_TLS_HOSTNAME_VERIFICATION,
                CONFIG_CIPHERS,

                // Pulsar Token Config
                CONFIG_ENABLE_TOKEN,
                CONFIG_AUTH_PLUGIN_PARAMS_STRING
        ));

    }

    @Override
    public String getId() {
        return this.id;
    }
}
