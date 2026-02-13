CREATE TABLE IF NOT EXISTS warehouse_products (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id UUID NOT NULL UNIQUE,
    quantity BIGINT NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    width DOUBLE PRECISION NOT NULL CHECK (width > 0),
    height DOUBLE PRECISION NOT NULL CHECK (height > 0),
    depth DOUBLE PRECISION NOT NULL CHECK (depth > 0),
    weight DOUBLE PRECISION NOT NULL CHECK (weight > 0),
    fragile BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_warehouse_products_product_id
    ON warehouse_products(product_id);
CREATE INDEX IF NOT EXISTS idx_warehouse_products_quantity
    ON warehouse_products(quantity);