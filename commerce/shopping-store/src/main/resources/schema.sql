CREATE TABLE IF NOT EXISTS products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    product_category VARCHAR(50) NOT NULL,
    image_src VARCHAR(500),
    quantity_state VARCHAR(20) NOT NULL,
    product_state VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    price NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_products_category_state ON products(product_category, product_state);
CREATE INDEX IF NOT EXISTS idx_products_state ON products(product_state);