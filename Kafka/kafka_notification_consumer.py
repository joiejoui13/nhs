import json

from confluent_kafka import Consumer, KafkaError


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "bank_transactions"
GROUP_ID = "notification-service"

# Bộ dữ liệu demo hiện có 12 giao dịch.
EXPECTED_MESSAGES = 12


def main():
    print("=== KAFKA CONSUMER - NOTIFICATION SERVICE ===")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([TOPIC])

    count = 0

    print(f"[Notify] Topic: {TOPIC}")
    print(f"[Notify] Consumer Group: {GROUP_ID}")
    print("[Notify] Đang chờ giao dịch...\n")

    try:
        while count < EXPECTED_MESSAGES:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue

                print(f"[Notify] Kafka error: {msg.error()}")
                continue

            transaction = json.loads(
                msg.value().decode("utf-8")
            )

            count += 1

            transaction_id = transaction["transaction_id"]
            customer_id = transaction["customer_id"]
            amount = transaction["amount"]

            print(
                f"[THÔNG BÁO] {customer_id} | "
                f"Giao dịch {transaction_id} thành công | "
                f"{amount:,} VND"
            )

        print("\n[Notify] Đã xử lý đủ dữ liệu demo.")

    except KeyboardInterrupt:
        print("\n[Notify] Dừng chương trình.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
