import os

# Đường dẫn tuyệt đối hoặc tương đối tới pizza_sales.csv
PIZZA_SALES = os.getenv("PIZZA_SALES_PATH", "./dataset/pizza_sales.csv")
