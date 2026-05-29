# Logstash Output Pulsar Plugin

This is a Java plugin for [Logstash](https://github.com/elastic/logstash) that writes events to [Apache Pulsar](https://pulsar.apache.org/) topics.

It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.

## Features

- Async message publishing with configurable send timeout
- Dynamic topic routing via Logstash `sprintf` format
- Retry with exponential backoff and jitter for transient failures
- Automatic producer reconnection on disconnect
- Graceful shutdown with in-flight message draining
- TLS keystore authentication
- Token-based authentication (JWT)
- Message compression (LZ4, ZLIB, ZSTD, SNAPPY)
- Configurable batching

This plugin uses **Pulsar Client 4.0.9**. For broker compatibility, see the [official Pulsar compatibility matrix](https://pulsar.apache.org/docs/client-libraries/).

## Configuration Options

| Setting | Type | Required | Default |
|---|:---:|:---:|---:|
| `serviceUrl` | string | No | `pulsar://localhost:6650` |
| `topic` | string | **Yes** | |
| `producer_name` | string | No | `logstash-pulsar` |
| `compression_type` | string | No | `NONE` |
| `block_if_queue_full` | boolean | No | `true` |
| `enable_batching` | boolean | No | `true` |
| `send_timeout_ms` | string | No | `30000` |
| `max_pending_messages` | string | No | `1000` |
| `max_retries` | string | No | `3` |
| `retry_initial_delay_ms` | string | No | `100` |
| `retry_max_delay_ms` | string | No | `10000` |
| `drain_timeout_ms` | string | No | `30000` |
| `enable_tls` | boolean | No | `false` |
| `tls_trust_store_path` | string | No | |
| `tls_trust_store_password` | string | No | |
| `enable_tls_hostname_verification` | boolean | No | `false` |
| `allow_tls_insecure_connection` | boolean | No | `false` |
| `protocols` | array | No | `["TLSv1.2"]` |
| `ciphers` | array | No | (see docs) |
| `auth_plugin_class_name` | string | No | `AuthenticationKeyStoreTls` |
| `enable_token` | boolean | No | `false` |
| `auth_plugin_params_string` | string | No | |

### Retry & Resilience Settings

| Setting | Description |
|---|---|
| `max_retries` | Number of retry attempts per message on transient failure (exponential backoff) |
| `retry_initial_delay_ms` | Initial retry delay; doubles each attempt with 25% jitter |
| `retry_max_delay_ms` | Cap on retry delay |
| `drain_timeout_ms` | Max time to wait for in-flight messages during shutdown |
| `send_timeout_ms` | Per-message send timeout; prevents deadlocks from indefinite blocking |

## Examples

### Basic (no auth)

```ruby
output {
  pulsar {
    serviceUrl => "pulsar://127.0.0.1:6650"
    topic => "persistent://public/default/%{topic_name}"
    producer_name => "my-logstash"
    enable_batching => true
    compression_type => "LZ4"
  }
}
```

### With retry tuning

```ruby
output {
  pulsar {
    serviceUrl => "pulsar://127.0.0.1:6650"
    topic => "persistent://public/default/logs"
    max_retries => "5"
    retry_initial_delay_ms => "200"
    retry_max_delay_ms => "30000"
    drain_timeout_ms => "60000"
    send_timeout_ms => "60000"
  }
}
```

### With token authentication

```ruby
output {
  pulsar {
    serviceUrl => "pulsar://localhost:6650"
    topic => "persistent://public/default/%{topic_name}"
    enable_batching => true
    enable_token => true
    auth_plugin_class_name => "org.apache.pulsar.client.impl.auth.AuthenticationToken"
    auth_plugin_params_string => "token:eyJhbGciOi..."
  }
}
```

### With TLS keystore authentication

```ruby
output {
  pulsar {
    serviceUrl => "pulsar+ssl://pulsar.example.com:6651"
    topic => "persistent://tenant/namespace/topic"
    enable_tls => true
    tls_trust_store_path => "/etc/pki/pulsar-truststore.jks"
    tls_trust_store_password => "changeit"
    enable_tls_hostname_verification => true
  }
}
```

## Building

Requires Logstash 7.17.x installed (for `logstash-core.jar`).

```bash
# Clone Logstash and build the core JAR
git clone https://github.com/elastic/logstash.git
cd logstash
git checkout tags/v7.17.6
./gradlew assemble

# In root of this repo:
./gradlew gem -PLOGSTASH_CORE_PATH=../logstash/logstash-core
```

The result is `logstash-output-pulsar-<version>.gem`.

## Installation

1. Install using the Logstash plugin manager:

```bash
/usr/share/logstash/bin/logstash-plugin install --no-verify --local logstash-output-pulsar-<version>.gem
```

## License

Apache License 2.0
