CREATE SCHEMA IF NOT EXISTS pizza_sales;

-- DIMENSION: dim_date
CREATE TABLE IF NOT EXISTS pizza_sales.dim_date (
    date_id INT PRIMARY KEY,
    order_date TIMESTAMP,
    day INT,
    month INT,
    year INT,
    quarter INT,
    week_of_year INT,
    weekday_num INT,
    weekday_name TEXT
);

-- DIMENSION: dim_time
CREATE TABLE IF NOT EXISTS pizza_sales.dim_time (
    time_id INT PRIMARY KEY,
    order_time TIMESTAMP,
    hour INT,
    minute INT,
    timeslot TEXT  
);

-- DIMENSION: dim_pizza
CREATE TABLE IF NOT EXISTS pizza_sales.dim_pizza (
    pizza_name_id TEXT NOT NULL,           
    pizza_base_id TEXT,                    
    pizza_size TEXT,                      
    unit_price DECIMAL(8,2),              
    pizza_ingredients TEXT,               
    pizza_name TEXT,
    pizza_sk BIGINT PRIMARY KEY          -- surrogate key
               
);

-- FACT: order_items
CREATE TABLE IF NOT EXISTS pizza_sales.fact_order_item (
    order_date TIMESTAMP,
    date_id INT,
    time_id INT,
    pizza_sk BIGINT,
    quantity INT,
    unit_price DECIMAL(8,2),
    total_price DECIMAL(10,2),
    
    -- Khóa ngoại (tuỳ chọn nếu bạn cần enforce)
    FOREIGN KEY (date_id) REFERENCES pizza_sales.dim_date(date_id),
    FOREIGN KEY (time_id) REFERENCES pizza_sales.dim_time(time_id),
    FOREIGN KEY (pizza_sk) REFERENCES pizza_sales.dim_pizza(pizza_sk)
);
