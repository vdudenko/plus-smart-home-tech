CREATE TABLE IF NOT EXISTS warehouse_products (
    id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL UNIQUE,
    quantity INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    width NUMERIC(10, 2) NOT NULL CHECK (width > 0),
    height NUMERIC(10, 2) NOT NULL CHECK (height > 0),
    depth NUMERIC(10, 2) NOT NULL CHECK (depth > 0),
    weight NUMERIC(10, 2) NOT NULL CHECK (weight > 0),
    fragile BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse_stock_history (
    id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL,
    quantity_change INTEGER NOT NULL,
    operation_type VARCHAR(20) NOT NULL, -- 'ADD', 'RESERVE', 'RELEASE'
    operation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_warehouse_products_product_id
    ON warehouse_products(product_id);
CREATE INDEX IF NOT EXISTS idx_warehouse_products_quantity
    ON warehouse_products(quantity);
CREATE INDEX IF NOT EXISTS idx_stock_history_product_id
    ON warehouse_stock_history(product_id);
CREATE INDEX IF NOT EXISTS idx_stock_history_operation_date
    ON warehouse_stock_history(operation_date);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_warehouse_products_updated_at
    BEFORE UPDATE ON warehouse_products
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE OR REPLACE FUNCTION log_stock_change()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.quantity != OLD.quantity THEN
        INSERT INTO warehouse_stock_history (
            product_id,
            quantity_change,
            operation_type,
            notes
        ) VALUES (
            NEW.product_id,
            NEW.quantity - OLD.quantity,
            CASE
                WHEN NEW.quantity > OLD.quantity THEN 'ADD'
                ELSE 'RESERVE'
            END,
            'Auto-logged stock change'
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER log_warehouse_stock_changes
    AFTER UPDATE ON warehouse_products
    FOR EACH ROW
    EXECUTE FUNCTION log_stock_change();