-- Item 4: tabela de referência de tipos de pagamento (dicionário TLC)
CREATE TABLE IF NOT EXISTS silver.payment_type (
    payment_type        INTEGER PRIMARY KEY,
    payment_description TEXT    NOT NULL,
    is_valid_payment    BOOLEAN NOT NULL
);

INSERT INTO silver.payment_type (payment_type, payment_description, is_valid_payment) VALUES
    (1, 'Credit card',  true),
    (2, 'Cash',         true),
    (3, 'No charge',    false),
    (4, 'Dispute',      false),
    (5, 'Unknown',      false),
    (6, 'Voided trip',  false)
ON CONFLICT (payment_type) DO NOTHING;