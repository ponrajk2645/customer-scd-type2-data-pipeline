CREATE TABLE IF NOT EXISTS stg_customer (
    customer_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    phone_number VARCHAR(20),
    email_verified TINYINT(1),
    city VARCHAR(50),
    plan VARCHAR(50),
    subscription_status VARCHAR(50),
    billing_cycle VARCHAR(50),
    event_date DATE NOT NULL,
    source_system VARCHAR(50),
    PRIMARY KEY (customer_id, event_date)
);
