# Projeto ETL de Vendas

Este projeto simula um pipeline de ETL simples usando Python, Pandas e PostgreSQL.

## Estrutura
- **Extract**: leitura de dados de um arquivo CSV
- **Transform**: tratamento de dados nulos e criação de campo `valor_total`
- **Load**: inserção dos dados em uma tabela PostgreSQL

## Como executar
1. Crie um banco PostgreSQL local
2. Execute o script em `sql/create_tables.sql`
3. Configure o acesso no `src/etl_vendas.py`
4. Rode o script ETL com:
```bash
python src/etl_vendas.py
```

## Exemplo de dados
Veja o arquivo `data/vendas_raw.csv`
