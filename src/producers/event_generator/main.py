"""Clickstream event generator. Produces realistic e-commerce events to Kafka."""
from __future__ import annotations

import json
import random
import signal
import time
from datetime import datetime, timezone
from uuid import UUID, uuid4

import structlog
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker

from src.producers.event_generator.config import settings
from src.producers.event_generator.schemas import ClickstreamEvent, EventType

log = structlog.get_logger()
fake = Faker()


# ─────────── Session simulation ───────────
class UserSession:
    """Tracks a user's session so events are causally consistent.

    A user must view a product before adding to cart, must add to cart
    before checking out, etc. Random events make for unrealistic data.
    """

    def __init__(self, user_id: int | None) -> None:
        self.user_id = user_id
        self.session_id: UUID = uuid4()
        self.viewed_products: list[int] = []
        self.cart: list[int] = []

    def next_event_type(self) -> EventType:
        """Return a plausible next event given current session state."""
        if not self.viewed_products:
            # First event must be a page view or product view
            return random.choices(
                [EventType.PAGE_VIEW, EventType.PRODUCT_VIEW],
                weights=[0.7, 0.3],
            )[0]

        # Weighted by config, but constrained by session state
        candidates = [EventType.PAGE_VIEW, EventType.PRODUCT_VIEW]
        weights = [settings.weight_page_view, settings.weight_product_view]

        if self.viewed_products:
            candidates.append(EventType.ADD_TO_CART)
            weights.append(settings.weight_add_to_cart)
        if self.cart:
            candidates.extend([EventType.REMOVE_FROM_CART, EventType.CHECKOUT_START])
            weights.extend([settings.weight_remove_from_cart, settings.weight_checkout_start])
        if self.cart and random.random() < 0.3:
            candidates.append(EventType.PURCHASE)
            weights.append(settings.weight_purchase)

        return random.choices(candidates, weights=weights)[0]


# ─────────── Event factory ───────────
def build_event(session: UserSession) -> ClickstreamEvent:
    event_type = session.next_event_type()
    product_id: int | None = None
    properties: dict = {}

    if event_type == EventType.PRODUCT_VIEW:
        product_id = random.randint(1, settings.num_products)
        session.viewed_products.append(product_id)
        properties = {"referrer": fake.uri()}
    elif event_type == EventType.ADD_TO_CART:
        product_id = random.choice(session.viewed_products)
        session.cart.append(product_id)
        properties = {"quantity": random.randint(1, 3), "price_cents": random.randint(500, 9999)}
    elif event_type == EventType.REMOVE_FROM_CART and session.cart:
        product_id = session.cart.pop()
    elif event_type == EventType.PURCHASE:
        properties = {
            "order_total_cents": sum(random.randint(500, 9999) for _ in session.cart),
            "items_count": len(session.cart),
        }
        session.cart.clear()
    elif event_type == EventType.PAGE_VIEW:
        properties = {"page": random.choice(["/", "/category/electronics", "/deals", "/search"])}

    return ClickstreamEvent(
        event_type=event_type,
        user_id=session.user_id,
        session_id=session.session_id,
        product_id=product_id,
        event_timestamp=datetime.now(timezone.utc),
        properties=properties,
    )


# ─────────── Kafka helpers ───────────
def ensure_topic_exists(bootstrap_servers: str, topic: str) -> None:
    """Create the topic if it doesn't exist. Idempotent."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=10)
    if topic in metadata.topics:
        log.info("topic_already_exists", topic=topic)
        return

    new_topic = NewTopic(topic, num_partitions=3, replication_factor=1)
    fs = admin.create_topics([new_topic])
    for t, f in fs.items():
        f.result()  # Raises on failure
        log.info("topic_created", topic=t, partitions=3)


def delivery_report(err, msg) -> None:
    """Async callback fired by confluent-kafka after each produce."""
    if err is not None:
        log.error("delivery_failed", error=str(err), topic=msg.topic())
    # On success we deliberately don't log — too noisy at 10+ events/sec


# ─────────── Main loop ───────────
def main() -> None:
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )
    log.info("starting_event_generator", config=settings.model_dump())

    ensure_topic_exists(settings.kafka_bootstrap_servers, settings.kafka_topic_clickstream)

    producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "ecommerce-event-generator",
        "compression.type": "snappy",
        "linger.ms": 10,           # Batch up to 10ms for throughput
        "acks": "all",             # Wait for full replication ack
        "enable.idempotence": True, # Exactly-once producer semantics
    })

    # Pre-create a pool of user sessions
    sessions: list[UserSession] = [
        UserSession(user_id=random.randint(1, settings.num_users) if random.random() > 0.1 else None)
        for _ in range(50)
    ]

    interval = 1.0 / settings.events_per_second
    start_time = time.time()
    event_count = 0
    running = True

    def shutdown_handler(signum, frame):
        nonlocal running
        log.info("shutdown_signal_received", signal=signum)
        running = False

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        while running:
            session = random.choice(sessions)
            # 5% chance to rotate this session out (simulate user leaving)
            if random.random() < 0.05:
                sessions.remove(session)
                sessions.append(UserSession(
                    user_id=random.randint(1, settings.num_users) if random.random() > 0.1 else None
                ))
                continue

            event = build_event(session)
            payload = event.model_dump_json().encode("utf-8")
            # Key by session_id so all events of one session land on same partition
            key = str(event.session_id).encode("utf-8")

            producer.produce(
                topic=settings.kafka_topic_clickstream,
                key=key,
                value=payload,
                callback=delivery_report,
            )
            producer.poll(0)  # Trigger delivery callbacks

            event_count += 1
            if event_count % 100 == 0:
                log.info("events_produced", count=event_count)

            if (
                settings.event_generator_duration_seconds > 0
                and time.time() - start_time > settings.event_generator_duration_seconds
            ):
                break

            time.sleep(interval)
    finally:
        log.info("flushing_producer")
        producer.flush(timeout=10)
        log.info("shutdown_complete", total_events=event_count)


if __name__ == "__main__":
    main()