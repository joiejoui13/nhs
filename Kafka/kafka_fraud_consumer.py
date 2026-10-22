import json

from confluent_kafka import Consumer, KafkaError


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "bank_transactions"
GROUP_ID = "fraud-detection-service"

# Bộ dữ liệu demo hiện có 12 giao dịch.
EXPECTED_MESSAGES = 12

# Luật demo đơn giản:
# Giao dịch >= 10 triệu đồng => cảnh báo.
FRAUD_THRESHOLD = 10000000


def main():
    print("=== KAFKA CONSUMER - FRAUD DETECTION ===")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([TOPIC])

    count = 0

    print(f"[Fraud] Topic: {TOPIC}")
    print(f"[Fraud] Consumer Group: {GROUP_ID}")
    print("[Fraud] Đang chờ giao dịch...\n")

    try:
        while count < EXPECTED_MESSAGES:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue

                print(f"[Fraud] Kafka error: {msg.error()}")
                continue

            transaction = json.loads(
                msg.value().decode("utf-8")
            )

            count += 1

            transaction_id = transaction["transaction_id"]
            amount = transaction["amount"]
            customer_id = transaction["customer_id"]

            if amount >= FRAUD_THRESHOLD:
                print(
                    f"[CẢNH BÁO] {transaction_id} | "
                    f"Khách hàng: {customer_id} | "
                    f"Số tiền: {amount:,} VND | "
                    f"Ngưỡng: {FRAUD_THRESHOLD:,} VND"
                )
            else:
                print(
                    f"[BÌNH THƯỜNG] {transaction_id} | "
                    f"Số tiền: {amount:,} VND"
                )

        print("\n[Fraud] Đã xử lý đủ dữ liệu demo.")

    except KeyboardInterrupt:
        print("\n[Fraud] Dừng chương trình.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
