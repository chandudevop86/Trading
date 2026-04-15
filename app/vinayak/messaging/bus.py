from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable
from urllib.parse import urlparse

from vinayak.core.config import get_settings

try:
    import pika
except Exception:  # pragma: no cover
    pika = None  # type: ignore

try:
    from kafka import KafkaConsumer, KafkaProducer
except Exception:  # pragma: no cover
    KafkaConsumer = None  # type: ignore
    KafkaProducer = None  # type: ignore

try:
    import stomp
except Exception:  # pragma: no cover
    stomp = None  # type: ignore


@dataclass(slots=True)
class EventEnvelope:
    name: str
    payload: dict[str, Any]
    emitted_at: str
    source: str


class MessageBus:
    def publish(self, name: str, payload: dict[str, Any], *, source: str) -> bool:
        raise NotImplementedError

    def consume(self, handler: Callable[[EventEnvelope], None]) -> None:
        raise NotImplementedError

    def readiness(self) -> dict[str, str]:
        raise NotImplementedError


class NoopMessageBus(MessageBus):
    def publish(self, name: str, payload: dict[str, Any], *, source: str) -> bool:
        return False

    def consume(self, handler: Callable[[EventEnvelope], None]) -> None:
        return None

    def readiness(self) -> dict[str, str]:
        return {'status': 'disabled', 'engine': 'noop'}


class RabbitMqMessageBus(MessageBus):
    def __init__(self, url: str, topic_prefix: str) -> None:
        self.url = url
        self.topic_prefix = topic_prefix
        self._connection = None
        self._channel = None

    def _routing_key(self, name: str) -> str:
        return f'{self.topic_prefix}.{name}'

    def _get_channel(self):
        if pika is None or not self.url:
            return None
        try:
            if self._connection is not None and self._channel is not None and self._connection.is_open and self._channel.is_open:
                return self._channel
        except Exception:
            self._connection = None
            self._channel = None

        params = pika.URLParameters(self.url)
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=self.topic_prefix, exchange_type='topic', durable=True)
        return self._channel

    def _close_publish_resources(self) -> None:
        try:
            if self._channel is not None and self._channel.is_open:
                self._channel.close()
        except Exception:
            pass
        try:
            if self._connection is not None and self._connection.is_open:
                self._connection.close()
        except Exception:
            pass
        self._channel = None
        self._connection = None

    def publish(self, name: str, payload: dict[str, Any], *, source: str) -> bool:
        if pika is None or not self.url:
            return False
        envelope = EventEnvelope(name=name, payload=payload, emitted_at=_now(), source=source)
        try:
            channel = self._get_channel()
            if channel is None:
                return False
            channel.basic_publish(
                exchange=self.topic_prefix,
                routing_key=self._routing_key(name),
                body=json.dumps(envelope.__dict__).encode('utf-8'),
                properties=pika.BasicProperties(content_type='application/json', delivery_mode=2),
            )
            return True
        except Exception:
            self._close_publish_resources()
            return False

    def consume(self, handler: Callable[[EventEnvelope], None]) -> None:
        if pika is None or not self.url:
            return None
        params = pika.URLParameters(self.url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(exchange=self.topic_prefix, exchange_type='topic', durable=True)
        queue_name = f'{self.topic_prefix}.worker'
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(queue=queue_name, exchange=self.topic_prefix, routing_key=f'{self.topic_prefix}.#')

        def _callback(ch, method, properties, body):
            try:
                data = json.loads(body.decode('utf-8'))
                handler(EventEnvelope(**data))
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        channel.basic_qos(prefetch_count=10)
        channel.basic_consume(queue=queue_name, on_message_callback=_callback)
        channel.start_consuming()

    def readiness(self) -> dict[str, str]:
        if pika is None or not self.url:
            return {'status': 'disabled', 'engine': 'rabbitmq'}
        try:
            params = pika.URLParameters(self.url)
            connection = pika.BlockingConnection(params)
            connection.close()
            return {'status': 'ok', 'engine': 'rabbitmq'}
        except Exception as exc:
            return {'status': 'error', 'engine': 'rabbitmq', 'detail': str(exc)}


class KafkaMessageBus(MessageBus):
    def __init__(self, url: str, topic_prefix: str) -> None:
        self.url = url
        self.topic_prefix = topic_prefix
        self._producer = None

    def _topic(self, name: str) -> str:
        return f'{self.topic_prefix}.{name}'

    def _get_producer(self):
        if KafkaProducer is None or not self.url:
            return None
        if self._producer is not None:
            return self._producer
        self._producer = KafkaProducer(
            bootstrap_servers=[self.url],
            value_serializer=lambda value: json.dumps(value).encode('utf-8'),
        )
        return self._producer

    def publish(self, name: str, payload: dict[str, Any], *, source: str) -> bool:
        if KafkaProducer is None or not self.url:
            return False
        envelope = EventEnvelope(name=name, payload=payload, emitted_at=_now(), source=source)
        try:
            producer = self._get_producer()
            if producer is None:
                return False
            producer.send(self._topic(name), envelope.__dict__).get(timeout=5)
            producer.flush()
            return True
        except Exception:
            try:
                if self._producer is not None:
                    self._producer.close()
            except Exception:
                pass
            self._producer = None
            return False

    def consume(self, handler: Callable[[EventEnvelope], None]) -> None:
        return None

    def readiness(self) -> dict[str, str]:
        if KafkaProducer is None or not self.url:
            return {'status': 'disabled', 'engine': 'kafka'}
        try:
            producer = KafkaProducer(bootstrap_servers=[self.url])
            producer.close()
            return {'status': 'ok', 'engine': 'kafka'}
        except Exception as exc:
            return {'status': 'error', 'engine': 'kafka', 'detail': str(exc)}


class ActiveMqMessageBus(MessageBus):
    def __init__(self, url: str, topic_prefix: str) -> None:
        self.url = url
        self.topic_prefix = topic_prefix
        self._client = None

    def _destination(self, name: str) -> str:
        return f'/topic/{self.topic_prefix}.{name}'

    def _connection(self):
        if stomp is None or not self.url:
            return None
        try:
            if self._client is not None and self._client.is_connected():
                return self._client
        except Exception:
            self._client = None
        parsed = urlparse(self.url)
        host = parsed.hostname or 'localhost'
        port = parsed.port or 61613
        conn = stomp.Connection12([(host, port)])
        username = parsed.username or 'admin'
        password = parsed.password or 'admin'
        conn.connect(login=username, passcode=password, wait=True)
        self._client = conn
        return conn

    def publish(self, name: str, payload: dict[str, Any], *, source: str) -> bool:
        conn = self._connection()
        if conn is None:
            return False
        envelope = EventEnvelope(name=name, payload=payload, emitted_at=_now(), source=source)
        try:
            conn.send(destination=self._destination(name), body=json.dumps(envelope.__dict__), headers={'persistent': 'true'})
            return True
        except Exception:
            try:
                conn.disconnect()
            except Exception:
                pass
            self._client = None
            return False

    def consume(self, handler: Callable[[EventEnvelope], None]) -> None:
        return None

    def readiness(self) -> dict[str, str]:
        conn = self._connection()
        if conn is None:
            return {'status': 'disabled', 'engine': 'activemq'}
        try:
            return {'status': 'ok', 'engine': 'activemq'}
        except Exception as exc:
            return {'status': 'error', 'engine': 'activemq', 'detail': str(exc)}


def _now() -> str:
    return datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')


def build_message_bus() -> MessageBus:
    settings = get_settings().message_bus
    if not settings.enabled:
        return NoopMessageBus()
    if settings.backend == 'rabbitmq':
        return RabbitMqMessageBus(settings.url, settings.topic_prefix)
    if settings.backend == 'kafka':
        return KafkaMessageBus(settings.url, settings.topic_prefix)
    if settings.backend == 'activemq':
        return ActiveMqMessageBus(settings.url, settings.topic_prefix)
    return NoopMessageBus()

