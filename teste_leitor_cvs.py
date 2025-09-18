import re
import sys
import pandas as pd
from pathlib import Path
import os
import io

def _find_onedrive_subfolder(subfolder_name: str):
    user_dir = os.environ.get("USERPROFILE", "")
    for nome in os.listdir(user_dir):
        if "DIAS BRANCO" in nome.upper():
            raiz = os.path.join(user_dir, nome)
            if os.path.isdir(raiz) and subfolder_name in os.listdir(raiz):
                return os.path.join(raiz, subfolder_name)
    return None

BASE_DIR_AUD = _find_onedrive_subfolder("Gestão de Estoque - Gestão_Auditoria")
if not BASE_DIR_AUD:
    raise FileNotFoundError("Pasta 'Gestão de Estoque - Gestão_Auditoria' não encontrada no OneDrive.")

caminho = os.path.join(BASE_DIR_AUD, "estoque_detalhado.csv")

EXPECTED_COLS = [
    "LOCAL_EXPEDICAO","COD_DEPOSITO","COD_ENDERECO","CHAVE_PALLET","VOLUME",
    "COD_ITEM","DESC_ITEM","LOTE","UOM","DOCUMENTO",
    "DATA_VALIDADE","DATA_ULTIMA_TRANSACAO","OCUPACAO",
    "CAPACIDADE","DESCRICAO","QTDE_POR_PALLET","PALLET_COMPLETO","BLOCO","TIPO_ENDERECO",
    "STATUS_PALLET","SHELF_ITEM","DATA_FABRICACAO","SHELF_ESTOQUE","DIAS_ESTOQUE","DIAS_VALIDADE","DATA_RELATORIO"
]

def _split_fix(parts, n):
    """Normaliza a lista 'parts' para ter exatamente n campos,
       unindo excedentes no último campo e preenchendo faltantes com vazio."""
    if len(parts) > n:
        head = parts[:n-1]
        tail = ";".join(parts[n-1:])
        return head + [tail]
    elif len(parts) < n:
        return parts + [""] * (n - len(parts))
    return parts

def _read_csv_strict_build_df(path, encodings=("utf-8-sig","utf-8","latin1","cp1252")):
    last_exc = None
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                lines = [ln.rstrip("\r\n") for ln in f]

            header_idx = None
            for i, ln in enumerate(lines):
                parts = ln.split(";")
                if ("LOCAL_EXPEDICAO" in parts) and ("COD_ENDERECO" in parts):
                    header_idx = i
                    break

            if header_idx is None:
                header_idx = 0

            header = _split_fix(lines[header_idx].split(";"), len(EXPECTED_COLS))
            use_cols = EXPECTED_COLS

            rows = []
            for ln in lines[header_idx+1:]:
                parts = _split_fix(ln.split(";"), len(EXPECTED_COLS))
                rows.append(parts)

            df_out = pd.DataFrame(rows, columns=use_cols, dtype=str)

            df_out.columns = [c.strip() for c in df_out.columns]
            for c in df_out.columns:
                if df_out[c].dtype == object:
                    df_out[c] = df_out[c].str.strip()

            return df_out
        except Exception as e:
            last_exc = e
            continue
    raise last_exc if last_exc else RuntimeError("Falha ao ler CSV com encodings testados.")

df = _read_csv_strict_build_df(caminho)

print("Colunas do CSV (ajustadas):")
print(df.columns.tolist())

print("\nPrimeiros itens:")
print(df.head())

if "BLOCO" in df.columns:
    print("\nValores da coluna BLOCO (top 10):")
    print(df["BLOCO"].head(10).tolist())
    print("dtype de ID:", df["BLOCO"].dtype)
else:
    print("\n[ALERTA] Coluna 'BLOCO' não encontrada; verifique o cabeçalho.")