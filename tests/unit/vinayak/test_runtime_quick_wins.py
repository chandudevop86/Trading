from __future__ import annotations

from vinayak.core.config import should_auto_initialize_database
from vinayak.messaging import bus as bus_module
from vinayak.messaging.bus import KafkaMessageBus, RabbitMqMessageBus


def test_should_auto_initialize_database_enabled_for_dev_and_sqlite() -> None:
    assert should_auto_initialize_database(env='development', provider='postgresql') is True
    assert should_auto_initialize_database(env='production', provider='sqlite') is True
    assert should_auto_initialize_database(env='uat', provider='sqlite') is True


def test_should_auto_initialize_database_disabled_for_nonlocal_managed_db() -> None:
    assert should_auto_initialize_database(env='production', provider='postgresql') is False
    assert should_auto_initialize_database(env='uat', provider='mysql') is False


def test_rabbitmq_message_bus_reuses_connection_for_publish(monkeypatch) -> None:
    published: list[tuple[str, str]] = []
    creation_count = {'count': 0}

    class FakeChannel:
        is_open = True

        def exchange_declare(self, **kwargs):
            return None

        def basic_publish(self, exchange, routing_key, body, properties):
            published.append((exchange, routing_key))

        def close(self):
            self.is_open = False

    class FakeConnection:
        is_open = True

        def __init__(self, params):
            creation_count['count'] += 1
            self._channel = FakeChannel()

        def channel(self):
            return self._channel

        def close(self):
            self.is_open = False

    class FakePika:
        class BasicProperties:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        @staticmethod
        def URLParameters(url):
            return url

        BlockingConnection = FakeConnection

    monkeypatch.setattr(bus_module, 'pika', FakePika)

    bus = RabbitMqMessageBus('amqp://guest:guest@localhost:5672/', 'vinayak')
    assert bus.publish('alpha', {'ok': 1}, source='test') is True
    assert bus.publish('beta', {'ok': 2}, source='test') is True
    assert creation_count['count'] == 1
    assert published == [('vinayak', 'vinayak.alpha'), ('vinayak', 'vinayak.beta')]


def test_kafka_message_bus_reuses_producer_for_publish(monkeypatch) -> None:
    creation_count = {'count': 0}
    sent_topics: list[str] = []

    class FakeFuture:
        def get(self, timeout):
            return True

    class FakeProducer:
        def __init__(self, **kwargs):
            creation_count['count'] += 1

        def send(self, topic, payload):
            sent_topics.append(topic)
            return FakeFuture()

        def flush(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(bus_module, 'KafkaProducer', FakeProducer)

    bus = KafkaMessageBus('localhost:9092', 'vinayak')
    assert bus.publish('alpha', {'ok': 1}, source='test') is True
    assert bus.publish('beta', {'ok': 2}, source='test') is True
    assert creation_count['count'] == 1
    assert sent_topics == ['vinayak.alpha', 'vinayak.beta']
