import pika
import json

# Kết nối tới RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Khai báo queue
channel.queue_declare(queue='data_queue')

# Hàm xử lý tin nhắn nhận được
def process_message(ch, method, properties, body):
    try:
        # Giải nén dữ liệu JSON
        data = json.loads(body)
        
        # Hiển thị dữ liệu nhận được
        print("Received data:")
        for key, value in data.items():
            print(f"{key}: {value}")
        print()
        
        # Xác nhận đã xử lý tin nhắn
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    except json.JSONDecodeError:
        print("Dữ liệu JSON không hợp lệ!")
        # Xác nhận đã xử lý tin nhắn lỗi
        ch.basic_ack(delivery_tag=method.delivery_tag)

# Đăng ký consumer
channel.basic_consume(queue='data_queue', on_message_callback=process_message)

# Bắt đầu tiêu thụ tin nhắn
channel.start_consuming()
