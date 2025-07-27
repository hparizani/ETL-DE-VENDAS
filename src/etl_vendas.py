import pandas as pd
import psycopg2

# Configurações do banco
DB_CONFIG = {
    "host": "localhost",
    "dbname": "vendas_db",
    "user": "seu_user",
    "password": "sua_senha"
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
    df_raw = extract("../data/vendas_raw.csv")
    df_clean = transform(df_raw)
    load(df_clean)
