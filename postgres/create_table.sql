CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(255) PRIMARY KEY,
    amount INTEGER,
    currency VARCHAR(10),
    created TIMESTAMP,
    network_status VARCHAR(50),
    risk_level VARCHAR(50),
    risk_score INTEGER,
    seller_message TEXT,
    paid BOOLEAN,
    card_brand VARCHAR(50),
    receipt_number VARCHAR(50),
    receipt_url TEXT
);