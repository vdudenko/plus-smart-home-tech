CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id UUID PRIMARY KEY,
    from_country VARCHAR(100) NOT NULL,
    from_city VARCHAR(100) NOT NULL,
    from_street VARCHAR(255) NOT NULL,
    from_house VARCHAR(50) NOT NULL,
    from_flat VARCHAR(50) NOT NULL,
    to_country VARCHAR(100) NOT NULL,
    to_city VARCHAR(100) NOT NULL,
    to_street VARCHAR(255) NOT NULL,
    to_house VARCHAR(50) NOT NULL,
    to_flat VARCHAR(50) NOT NULL,
    order_id UUID NOT NULL UNIQUE REFERENCES orders(order_id) ON DELETE CASCADE,
    delivery_state VARCHAR(20) NOT NULL CHECK (delivery_state IN ('CREATED', 'IN_PROGRESS', 'DELIVERED', 'FAILED', 'CANCELLED'))
);

CREATE INDEX IF NOT EXISTS idx_deliveries_order_id ON deliveries(order_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_state ON deliveries(delivery_state);