import json
import time
from pathlib import Path

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "bank_transactions"
PARTITIONS = 2
DATA_FILE = Path(__file__).with_name("bank_transactions_demo.json")


def create_topic_if_needed():
    """Tạo topic nếu topic chưa tồn tại."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    try:
        futures = admin.create_topics(
            [NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)]
        )
        futures[TOPIC].result()
        print(f"[Kafka] Đã tạo topic: {TOPIC}")
    except KafkaException as e:
        if "TOPIC_ALREADY_EXISTS" in str(e):
            print(f"[Kafka] Topic đã tồn tại: {TOPIC}")
        else:
            raise


def delivery_report(err, msg):
    """Callback báo kết quả gửi message."""
    if err is not None:
        print(f"[Producer] Lỗi gửi {msg.key()}: {err}")
        return

    print(
        f"[Producer] {msg.key()} -> "
        f"topic={msg.topic()}, "
        f"partition={msg.partition()}, "
        f"offset={msg.offset()}"
    )


def load_transactions():
    """Đọc danh sách giao dịch từ file JSON."""
    if not DATA_FILE.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file dữ liệu: {DATA_FILE}"
        )

    with DATA_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("File JSON phải chứa một danh sách giao dịch.")

    return data


def main():
    print("=== KAFKA PRODUCER - GIAO DỊCH NGÂN HÀNG ===")

    create_topic_if_needed()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "bank-transaction-producer",
    })

    transactions = load_transactions()

    print(f"[Producer] Có {len(transactions)} giao dịch.")
    print("[Producer] Bắt đầu gửi...\n")

    try:
        for transaction in transactions:
            transaction_id = transaction["transaction_id"]
            customer_id = transaction["customer_id"]

            producer.produce(
                topic=TOPIC,
                key=customer_id,
                value=json.dumps(
                    transaction,
                    ensure_ascii=False
                ).encode("utf-8"),
                callback=delivery_report,
            )

            # Cho callback được xử lý và tạo cảm giác stream real-time.
            producer.poll(0)

            print(
                f"  Gửi {transaction_id}: "
                f"{transaction['amount']:,} {transaction['currency']}"
            )

            time.sleep(1)

        # Chờ tất cả message được gửi xong.
        remaining = producer.flush()

        if remaining == 0:
            print("\n[Producer] Đã gửi toàn bộ giao dịch.")
        else:
            print(
                f"\n[Producer] Còn {remaining} message chưa gửi được."
            )

    except KeyboardInterrupt:
        print("\n[Producer] Dừng chương trình.")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()
