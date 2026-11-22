import threading

from confluent_kafka import KafkaError

from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents


def test_take_codes_returns_and_resets() -> None:
    events = KafkaErrorEvents()
    assert events.take_codes() == frozenset()
    events.record(KafkaError(KafkaError._AUTHENTICATION))
    events.record(KafkaError(KafkaError._AUTHENTICATION))
    events.record(KafkaError(KafkaError._ALL_BROKERS_DOWN))
    assert events.take_codes() == {
        KafkaError._AUTHENTICATION,
        KafkaError._ALL_BROKERS_DOWN,
    }
    assert events.take_codes() == frozenset()


def test_record_is_thread_safe() -> None:
    events = KafkaErrorEvents()
    threads = [
        threading.Thread(
            target=lambda: [events.record(KafkaError(c)) for c in range(-200, -100)]
        )
        for _ in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(events.take_codes()) == 100
