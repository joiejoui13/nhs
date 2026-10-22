import json
import time
from pathlib import Path

import pika


RABBITMQ_HOST = "localhost"
QUEUE = "bank_transactions"
DATA_FILE = Path(__file__).with_name("bank_transactions_demo.json")


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
    print("=== RABBITMQ PRODUCER - GIAO DỊCH NGÂN HÀNG ===")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()

    # Queue tồn tại bền vững để minh họa message queue.
    channel.queue_declare(
        queue=QUEUE,
        durable=True
    )

    transactions = load_transactions()

    print(f"[Producer] Có {len(transactions)} giao dịch.")
    print("[Producer] Bắt đầu gửi vào queue...\n")

    try:
        for transaction in transactions:
            transaction_id = transaction["transaction_id"]

            body = json.dumps(
                transaction,
                ensure_ascii=False
            ).encode("utf-8")

            channel.basic_publish(
                exchange="",
                routing_key=QUEUE,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                )
            )

            print(
                f"[Producer] Đã gửi {transaction_id}: "
                f"{transaction['amount']:,} {transaction['currency']}"
            )

            # Tạo cảm giác dữ liệu đến theo thời gian thực.
            time.sleep(1)

        print("\n[Producer] Đã gửi toàn bộ giao dịch.")

    except KeyboardInterrupt:
        print("\n[Producer] Dừng chương trình.")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
