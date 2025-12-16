import pandas as pd
import sqlite3


def extrair():
    df = pd.read_csv("vendas.csv")
    return df


def transformar(df):
    df["data_venda"] = pd.to_datetime(
        df["data_venda"],
        errors="coerce",
        dayfirst=True
    )
    df["data_venda"] = df["data_venda"].dt.strftime("%Y-%m-%d")

    df["categoria"] = df["categoria"].fillna("Não informado")

    df["quantidade"] = (
        df["quantidade"]
        .astype(str)
        .str.lower()
        .str.replace("três", "3", regex=False)
        .str.replace("tres", "3", regex=False)
        .str.replace("trs", "3", regex=False)
        .str.replace("-", "", regex=False)
    )

    print(df["quantidade"].unique()[:20])

    df["quantidade"] = df["quantidade"].astype(float).astype("Int64")

    df["preco_unitario"] = (
        df["preco_unitario"]
        .astype(str)
        .str.replace("trs", "", regex=False)
        .astype(float)
    )
    df = df[df["preco_unitario"] >= 0]

    df["valor_total"] = df["quantidade"] * df["preco_unitario"]

    return df


def carregar(df):
    con = sqlite3.connect("vendas.db")
    df.to_sql("tbvendas", con, if_exists="replace", index=False)

    con.execute("DROP TABLE IF EXISTS tbclientes;")
    sql_tbclientes = """
    CREATE TABLE tbclientes AS
    SELECT DISTINCT
        cliente AS id_cliente,
        cliente AS nome_cliente
    FROM tbvendas;
    """
    con.execute(sql_tbclientes)
    con.close()


def etl():
    dados = extrair()
    dados_tratados = transformar(dados)
    carregar(dados_tratados)


if __name__ == "__main__":
    etl()
