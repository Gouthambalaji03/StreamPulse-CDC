-- PostgreSQL CDC Initialization Script
-- Configure logical replication and prepare CDC tables

-- Ensure replication user and publication exist
-- wal_level = logical must be set in postgresql.conf

CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    role VARCHAR(32) DEFAULT 'member',
    status VARCHAR(32) DEFAULT 'active',
    last_login TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    status VARCHAR(32) DEFAULT 'pending',
    carrier VARCHAR(64),
    tracking_num VARCHAR(128),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sku VARCHAR(64) NOT NULL UNIQUE,
    price NUMERIC(10, 2) NOT NULL,
    stock INTEGER NOT NULL DEFAULT 0,
    category VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS payments (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    method VARCHAR(32) NOT NULL,
    status VARCHAR(32) DEFAULT 'captured',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventory (
    id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL,
    warehouse VARCHAR(64) NOT NULL,
    quantity INTEGER NOT NULL,
    reserved INTEGER DEFAULT 0
);

-- Crucial for CDC: Configure REPLICA IDENTITY to FULL so WAL logs both before and after states
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE payments REPLICA IDENTITY FULL;
ALTER TABLE inventory REPLICA IDENTITY FULL;

-- Create Publication for Logical Replication
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'cdc_publication') THEN
        CREATE PUBLICATION cdc_publication FOR ALL TABLES;
    END IF;
END $$;

-- Insert seed rows matching the demo snapshot
INSERT INTO users (id, name, email, role, status, last_login) VALUES
(1023, 'Elena Fisher', 'elena.f@enterprise.io', 'lead', 'active', '2024-01-08 10:24:26+00'),
(1024, 'John Doe', 'john.doe@enterprise.io', 'engineer', 'active', CURRENT_TIMESTAMP)
ON CONFLICT (id) DO NOTHING;

INSERT INTO orders (id, customer_id, amount, status, carrier, tracking_num) VALUES
(8932, 1024, 249.99, 'shipped', 'FedEx', 'FX-8849201')
ON CONFLICT (id) DO NOTHING;

INSERT INTO payments (id, order_id, amount, currency, method, status) VALUES
(7221, 8932, 129.99, 'USD', 'stripe', 'captured')
ON CONFLICT (id) DO NOTHING;

INSERT INTO products (id, name, sku, price, stock, category) VALUES
(554, 'Mechanical Keyboard TKL', 'PRD-554-BLK', 149.00, 35, 'Peripherals'),
(555, 'Ergonomic Monitor Arm', 'PRD-555-SLV', 89.99, 142, 'Accessories')
ON CONFLICT (id) DO NOTHING;
