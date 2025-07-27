CREATE TABLE vendas (
    id_venda INTEGER PRIMARY KEY,
    data_venda DATE,
    produto TEXT,
    quantidade INTEGER,
    preco_unitario NUMERIC,
    cliente TEXT,
    valor_total NUMERIC
);
