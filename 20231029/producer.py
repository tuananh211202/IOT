import pika
import json

# Kết nối tới RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Khai báo queue
channel.queue_declare(queue='data_queue')

# Nhập số trường từ người dùng
num_fields = int(input("Nhập số trường: "))

# Tạo dictionary để lưu trữ dữ liệu
data = {}

# Nhập tên và giá trị của từng trường
for i in range(num_fields):
    field_name = input(f"Nhập tên trường {i + 1}: ")
    field_value = input(f"Nhập giá trị trường {i + 1}: ")
    data[field_name] = field_value

# Đóng gói dữ liệu thành JSON
json_data = json.dumps(data)

# Gửi dữ liệu đến queue
channel.basic_publish(exchange='', routing_key='data_queue', body=json_data)

print("Dữ liệu đã được gửi thành công!")

# Đóng kết nối
connection.close()
