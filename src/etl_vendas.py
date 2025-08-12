import pandas as pd
import psycopg2
import sys
from pathlib import Path

# Configurações do banco
DB_CONFIG = {
    "host": "localhost",
    "dbname": "db_nome",
    "user": "seu_user",
    "password": "sua_password"
}

def extract(path):
    return pd.read_csv(path, sep=",", encoding="utf-8")

def transform(df):
    df["data_venda"] = pd.to_datetime(df["data_venda"], errors="coerce")
    df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0)
    df["preco_unitario"] = pd.to_numeric(df["preco_unitario"], errors="coerce").fillna(0)
    df["valor_total"] = df["quantidade"] * df["preco_unitario"]
    df = df.dropna(subset=["data_venda"])
    return df

def load(df, table_name="vendas"):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(f"""
            INSERT INTO {table_name} (id_venda, data_venda, produto, quantidade, preco_unitario, cliente, valor_total)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    DEFAULT_CSV = PROJECT_ROOT / "data" / "vendas_raw.csv"

    # permite passar o caminho do CSV via argumento; senão usa o padrão
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV

    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV não encontrado em: {csv_path}\n"
            f"Dica: rode 'python src/etl_vendas.py' a partir da raiz do projeto "
            f"ou passe o caminho: 'python src/etl_vendas.py data/vendas_raw.csv'"
        )

    df_raw = extract(csv_path)
    df_clean = transform(df_raw)
    load(df_clean)
