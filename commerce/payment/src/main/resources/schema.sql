CREATE TABLE IF NOT EXISTS payments (
    payment_id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE REFERENCES orders(order_id) ON DELETE CASCADE,
    product_total DOUBLE PRECISION NOT NULL,
    delivery_total DOUBLE PRECISION NOT NULL,
    fee_total DOUBLE PRECISION NOT NULL,
    total_payment DOUBLE PRECISION NOT NULL,
    state VARCHAR(20) NOT NULL CHECK (state IN ('PENDING', 'SUCCESS', 'FAILED'))
);

CREATE INDEX idx_payments_order_id ON payments(order_id);
CREATE INDEX idx_payments_state ON payments(state);