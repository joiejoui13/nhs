import json
import sys
import time

import pika


RABBITMQ_HOST = "localhost"
QUEUE = "bank_transactions"
EXPECTED_MESSAGES = 6


def main():
    # Có thể chạy:
    # python rabbitmq_worker.py Worker-1
    # python rabbitmq_worker.py Worker-2
    worker_name = sys.argv[1] if len(sys.argv) > 1 else "Worker"

    print(f"=== {worker_name.upper()} - RABBITMQ WORKER ===")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()

    channel.queue_declare(
        queue=QUEUE,
        durable=True
    )

    # Mỗi worker nhận tối đa 1 message chưa ACK tại một thời điểm.
    # Khi có nhiều worker cùng queue, RabbitMQ sẽ phân phối công việc
    # cho các worker đang sẵn sàng xử lý.
    channel.basic_qos(prefetch_count=1)

    processed = 0

    def callback(ch, method, properties, body):
        nonlocal processed

        try:
            transaction = json.loads(
                body.decode("utf-8")
            )

            processed += 1

            print(
                f"[{worker_name}] nhận "
                f"{transaction['transaction_id']} | "
                f"{transaction['customer_id']} | "
                f"{transaction['amount']:,} VND"
            )

            # Giả lập xử lý nghiệp vụ.
            time.sleep(2)

            print(
                f"[{worker_name}] xử lý xong "
                f"{transaction['transaction_id']}"
            )

            # ACK sau khi xử lý thành công.
            ch.basic_ack(
                delivery_tag=method.delivery_tag
            )

            if processed >= EXPECTED_MESSAGES:
                print(
                    f"[{worker_name}] Đã xử lý "
                    f"{EXPECTED_MESSAGES} message trong phiên demo."
                )

        except (json.JSONDecodeError, KeyError) as e:
            print(f"[{worker_name}] Dữ liệu không hợp lệ: {e}")

            # Message lỗi được ACK để tránh vòng lặp vô hạn.
            ch.basic_ack(
                delivery_tag=method.delivery_tag
            )

    channel.basic_consume(
        queue=QUEUE,
        on_message_callback=callback
    )

    print(f"[{worker_name}] Queue: {QUEUE}")
    print(f"[{worker_name}] Đang chờ message...\n")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"\n[{worker_name}] Dừng chương trình.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
