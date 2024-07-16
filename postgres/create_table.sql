CREATE TABLE sentences (
    id SERIAL PRIMARY KEY,
    sentence VARCHAR(10000) NOT NULL,
    sentiment FLOAT
    );
)