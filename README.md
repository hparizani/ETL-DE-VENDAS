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
4. Rode o script ETL a partir da raiz do projeto. O script procura o arquivo CSV
   padrão em `data/vendas_raw.csv`, mas você pode informar um caminho
   diferente como argumento:
```bash
# usando o CSV padrão
python src/etl_vendas.py

# ou apontando para outro CSV
python src/etl_vendas.py caminho/para/seu_arquivo.csv
```

## Exemplo de dados
Veja o arquivo `data/vendas_raw.csv`
