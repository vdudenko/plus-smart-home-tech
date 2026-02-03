CREATE TABLE IF NOT EXISTS carts (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_username_active UNIQUE (username, active)
);

CREATE TABLE IF NOT EXISTS cart_items (
    id BIGSERIAL PRIMARY KEY,
    cart_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_cart_items_cart
        FOREIGN KEY (cart_id)
        REFERENCES carts(id)
        ON DELETE CASCADE,
    CONSTRAINT uk_cart_product UNIQUE (cart_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_carts_username_active
    ON carts(username, active);
CREATE INDEX IF NOT EXISTS idx_carts_active
    ON carts(active);
CREATE INDEX IF NOT EXISTS idx_cart_items_cart_id
    ON cart_items(cart_id);
CREATE INDEX IF NOT EXISTS idx_cart_items_product_id
    ON cart_items(product_id);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_carts_updated_at
    BEFORE UPDATE ON carts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE OR REPLACE FUNCTION update_added_at_column()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.added_at IS NULL THEN
        NEW.added_at = CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_cart_items_added_at
    BEFORE INSERT ON cart_items
    FOR EACH ROW
    EXECUTE FUNCTION update_added_at_column();