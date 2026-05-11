-- E-commerce operational schema
CREATE SCHEMA IF NOT EXISTS ecommerce;

CREATE TABLE ecommerce.users (
    user_id BIGSERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255),
    country VARCHAR(2),
    signup_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ecommerce.products (
    product_id BIGSERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price_cents INTEGER NOT NULL CHECK (price_cents >= 0),
    inventory_count INTEGER DEFAULT 0
);

CREATE TABLE ecommerce.orders (
    order_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES ecommerce.users(user_id),
    order_status VARCHAR(20) NOT NULL,
    total_cents INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ecommerce.order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT REFERENCES ecommerce.orders(order_id),
    product_id BIGINT REFERENCES ecommerce.products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents INTEGER NOT NULL
);

CREATE INDEX idx_orders_user_id ON ecommerce.orders(user_id);
CREATE INDEX idx_orders_created_at ON ecommerce.orders(created_at);
CREATE INDEX idx_order_items_order_id ON ecommerce.order_items(order_id);