import csv
from faker import Faker

fake = Faker()

num_records = 10
fields = ['id', 'name', 'age', 'city', 'salary']
with open('employees.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=fields)
    writer.writeheader()
    for _ in range(num_records):
        writer.writerow({
            'id': fake.uuid4(),  # ID duy nhất
            'name': fake.name(),  # Tên ngẫu nhiên
            'age': fake.random_int(min=18, max=65),  # Tuổi từ 18 đến 65
            'city': fake.city(),  # Thành phố ngẫu nhiên
            'salary': fake.random_int(min=20000, max=100000)  # Mức lương từ 20,000 đến 100,000
        })

print("Dữ liệu đã được phát sinh và lưu vào file employees.csv")
