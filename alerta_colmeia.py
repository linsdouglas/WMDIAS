import pandas as pd
import numpy as np
import os
import yagmail
from datetime import datetime
import logging
import time
import csv
import unicodedata, re


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"estoque_alarme_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def log(mensagem):
    logger.info(mensagem)

DEBUG_DIR = os.path.join(os.getcwd(), f"_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
os.makedirs(DEBUG_DIR, exist_ok=True)
log(f"[DEBUG] Salvando arquivos de diagnóstico em: {DEBUG_DIR}")

def _dump(df, name, index=False):
    path = os.path.join(DEBUG_DIR, name)
    try:
        df.to_csv(path, sep=";", index=index, encoding="utf-8-sig")
        log(f"[DEBUG] Exportado: {path} ({len(df)} linhas)")
    except Exception as e:
        log(f"[WARN] Falha ao exportar {name}: {e}")

def salvar_estoque_posicao_no_diretorio(base_dir: str, estoque_posicao: pd.DataFrame, salvar_xlsx: bool = False) -> str:

    df = estoque_posicao.copy()

    for col in ("DATA_VALIDADE", "DATA_PRIMEIRO_PALLET"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    col_prio = [
        "BLOCO","COD_ENDERECO","COD_ITEM","DATA_VALIDADE",
        "OCUPACAO","CAPACIDADE","LIVRE","FLAG_CHEIO",
        "CHAVE_SKU_DATA","CHAVE_POS",
        "ISFRONT_RUASKU","DATA_PRIMEIRO_PALLET","TOLERANCIA_5_DIAS",
        "HABILITA_ORIGEM_FRONT","HABILITA_DESTINO_FRONT",
        "TAXA_OCUPACAO_ORDENACAO","INDICE_GRUPO_NF_GLOBAL",
        "OCP_REDISTRIBUIVEL_GLOBAL","OCP_CAP_ACUMULADA_NF_ANTES_GLOBAL",
        "OCP_OCUPACAO_OTIMA_GLOBAL"
    ]
    cols_final = [c for c in col_prio if c in df.columns] + [c for c in df.columns if c not in col_prio]
    df = df[cols_final]

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(base_dir, f"estoque_posicao_{ts}.csv")

    try:
        df.to_csv(csv_path, sep=";", index=False, encoding="utf-8-sig")
        log(f"[EXPORT] estoque_posicao salvo em CSV: {csv_path}  (linhas: {len(df)})")
    except Exception as e:
        log(f"[EXPORT][ERRO] Falha ao salvar CSV: {e}")
        csv_path = ""

    if salvar_xlsx:
        try:
            xlsx_path = os.path.join(base_dir, f"estoque_posicao_{ts}.xlsx")
            df.to_excel(xlsx_path, index=False)
            log(f"[EXPORT] estoque_posicao salvo em XLSX: {xlsx_path}")
        except Exception as e:
            log(f"[EXPORT][ERRO] Falha ao salvar XLSX: {e}")

    return csv_path


def encontrar_pasta_onedrive_empresa():
    log("Procurando pasta do OneDrive da empresa...")
    user_dir = os.environ["USERPROFILE"]
    possiveis = os.listdir(user_dir)
    
    for nome in possiveis:
        if "DIAS BRANCO" in nome.upper():
            caminho_completo = os.path.join(user_dir, nome)
            onedrive_dir = os.path.join(caminho_completo, "Gestão de Estoque - Gestão_Auditoria")
            if os.path.isdir(onedrive_dir):
                log(f"Pasta do OneDrive encontrada: {onedrive_dir}")
                return onedrive_dir
    
    log("Pasta do OneDrive não encontrada!")
    return None

EXPECTED_COLS = [
    "LOCAL_EXPEDICAO","COD_DEPOSITO","COD_ENDERECO","CHAVE_PALLET","VOLUME",
    "COD_ITEM","DESC_ITEM","LOTE","UOM","DOCUMENTO",
    "DATA_VALIDADE","DATA_ULTIMA_TRANSACAO","OCUPACAO",
    "CAPACIDADE","DESCRICAO","QTDE_POR_PALLET","PALLET_COMPLETO","BLOCO","TIPO_ENDERECO",
    "STATUS_PALLET","SHELF_ITEM","DATA_FABRICACAO","SHELF_ESTOQUE","DIAS_ESTOQUE","DIAS_VALIDADE","DATA_RELATORIO"
]

def _split_fix(parts, n):
    if len(parts) > n:
        head = parts[:n-1]
        tail = ";".join(parts[n-1:])
        return head + [tail]
    elif len(parts) < n:
        return parts + [""] * (n - len(parts))
    return parts

def _read_csv_strict_build_df(path, encodings=("utf-8-sig","utf-8","latin1","cp1252")) -> pd.DataFrame:
    last_exc = None
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                lines = [ln.rstrip("\r\n") for ln in f if ln.strip()]
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
                df_out[c] = df_out[c].astype(str).str.strip().str.replace('"','').str.replace("'","")
            return df_out
        except Exception as e:
            last_exc = e
            continue
    raise last_exc if last_exc else RuntimeError("Falha ao ler CSV com encodings testados.")

def _pick_col(df, candidatos):
    log(f"Procurando coluna entre candidatos: {candidatos}")
    cols = {c.strip().lower(): c for c in df.columns}
    
    for cand in candidatos:
        k = cand.strip().lower()
        if k in cols:
            log(f"Coluna encontrada: {cols[k]}")
            return cols[k]
    
    norm_cands = {cand.strip().lower().replace(" ", "_") for cand in candidatos}
    for c in df.columns:
        if c.strip().lower().replace(" ", "_") in norm_cands:
            log(f"Coluna encontrada (após normalização): {c}")
            return c
    
    raise ValueError(f"Coluna não encontrada. Candidatos: {candidatos}")

def ler_csv_corretamente(csv_path):
    log(f"Processando arquivo (estrito): {os.path.basename(csv_path)}")
    df = _read_csv_strict_build_df(csv_path)

    df.columns = [c.upper() for c in df.columns]

    for col in ("DATA_VALIDADE","DATA_ULTIMA_TRANSACAO","DATA_FABRICACAO","DATA_RELATORIO"):
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
            except Exception:
                pass

    for col in ("OCUPACAO","CAPACIDADE","QTDE_POR_PALLET","VOLUME","DIAS_ESTOQUE","DIAS_VALIDADE"):
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(r"[^0-9\.\-]", "", regex=True)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    log(f"CSV lido (estrito) — Linhas: {len(df)}, Colunas: {list(df.columns)}")
    return df

def verificar_qualidade_dados(df, nome_arquivo):
    log(f"Verificando qualidade dos dados de {nome_arquivo}...")
    
    nulos_por_coluna = df.isnull().sum()
    if nulos_por_coluna.any():
        log(f"Valores nulos por coluna:")
        for col, count in nulos_por_coluna.items():
            if count > 0:
                log(f"  {col}: {count} nulos ({count/len(df)*100:.1f}%)")
    
    log(f"Tipos de dados:")
    for col, dtype in df.dtypes.items():
        log(f"  {col}: {dtype}")
    
    log(f"Primeiros valores de cada coluna:")
    for col in df.columns:
        valores_unicos = df[col].nunique()
        primeiros_valores = df[col].dropna().head(3).tolist()
        log(f"  {col}: {valores_unicos} valores únicos, exemplos: {primeiros_valores}")
    
    return df
    
def calcular_posicao_real_por_palete(estoque_df):
    log("Calculando Posicao_Real_Por_Palete...")
    
    try:
        coluna_data_transacao = _pick_col(estoque_df, ['DATA_ULTIMA_TRANSACAO', 'DT_ULTIMA_TRANSACAO', 'ULTIMA_TRANSACAO', 'DATA_TRANSACAO'])
        log(f"Coluna de data de transação encontrada: {coluna_data_transacao}")
    except ValueError:
        log("ERRO: Coluna de data da última transação não encontrada!")
        raise ValueError("Coluna DATA_ULTIMA_TRANSACAO não encontrada")
    
    log("Convertendo coluna de data para datetime preservando hora exata...")
    
    formatos = [
        '%d/%m/%Y %H:%M:%S',    # 20/08/2025 19:38:59
        '%d/%m/%Y %H:%M',       # 20/08/2025 19:38
        '%d/%m/%Y',             # 20/08/2025
        '%Y-%m-%d %H:%M:%S',    # 2025-08-20 19:38:59
        '%Y-%m-%d %H:%M',       # 2025-08-20 19:38
        '%Y-%m-%d',             # 2025-08-20
        '%m/%d/%Y %H:%M:%S',    # 08/20/2025 19:38:59 (formato US)
        '%m/%d/%Y %H:%M',       # 08/20/2025 19:38 (formato US)
        '%m/%d/%Y'              # 08/20/2025 (formato US)
    ]
    
    data_convertida = None
    for formato in formatos:
        try:
            data_convertida = pd.to_datetime(
                estoque_df[coluna_data_transacao], 
                format=formato,
                errors='raise'
            )
            log(f"Formato identificado: {formato}")
            break
        except:
            continue
    
    if data_convertida is None:
        log("Inferindo formato automaticamente...")
        data_convertida = pd.to_datetime(
            estoque_df[coluna_data_transacao], 
            infer_datetime_format=True,
            dayfirst=True,  
            errors='coerce'
        )
    
    estoque_df[coluna_data_transacao] = data_convertida
    
    nulos = estoque_df[coluna_data_transacao].isnull().sum()
    if nulos > 0:
        log(f"AVISO: {nulos} datas não puderam ser convertidas")
        datas_problematicas = estoque_df[estoque_df[coluna_data_transacao].isnull()][coluna_data_transacao].unique()[:5]
        log(f"Exemplos de datas problemáticas: {datas_problematicas}")
    
    tem_hora = estoque_df[coluna_data_transacao].dt.time.nunique() > 1
    if not tem_hora:
        log("AVISO: As datas não possuem informação de hora diferenciada")
        log("Será necessário usar um critério adicional para desempate")
        
        estoque_df['DESEMPATE'] = estoque_df.index
        coluna_ranking = ['DESEMPATE']
    else:
        coluna_ranking = [coluna_data_transacao]
    
    log("Calculando ranking por endereço e SKU...")
    
    colunas_ordenacao = ['COD_ENDERECO', 'COD_ITEM'] + coluna_ranking
    ascending = [True, True, False]  #
    
    estoque_df.sort_values(colunas_ordenacao, ascending=ascending, inplace=True)
    
    if tem_hora:
        estoque_df['POSICAO_REAL_POR_PALLETE'] = estoque_df.groupby(
            ['COD_ENDERECO', 'COD_ITEM']
        )[coluna_data_transacao].rank(method='dense', ascending=False).astype(int)
    else:
        estoque_df['POSICAO_REAL_POR_PALLETE'] = estoque_df.groupby(
            ['COD_ENDERECO', 'COD_ITEM']
        )['DESEMPATE'].rank(method='first', ascending=True).astype(int)
    
    if 'DESEMPATE' in estoque_df.columns:
        estoque_df.drop('DESEMPATE', axis=1, inplace=True)
    
    log("Posicao_Real_Por_Palete calculada com sucesso")
    
    exemplo_ranking = estoque_df.groupby(['COD_ENDERECO', 'COD_ITEM']).size().reset_index(name='count')
    log(f"Total de grupos únicos: {len(exemplo_ranking)}")
    log(f"Exemplo de ranking - Primeiras posições:")
    for _, row in estoque_df[estoque_df['POSICAO_REAL_POR_PALLETE'] == 1].head(3).iterrows():
        log(f"  Endereço: {row['COD_ENDERECO']}, SKU: {row['COD_ITEM']}, Data: {row[coluna_data_transacao]}, Posição: {row['POSICAO_REAL_POR_PALLETE']}")
    
    return estoque_df

def calcular_data_primeiro_palete(estoque_df):
    log("Calculando Data_Primeiro_Palete...")
    
    estoque_df = calcular_posicao_real_por_palete(estoque_df)
    
    log("Filtrando primeiras posições de palete...")
    primeiras_posicoes = estoque_df[estoque_df['POSICAO_REAL_POR_PALLETE'] == 1].copy()
    
    log("Agrupando por endereço e SKU...")
    data_primeiro_palete = primeiras_posicoes.groupby(['COD_ENDERECO', 'COD_ITEM']).agg(
        DATA_PRIMEIRO_PALLET=('DATA_VALIDADE', 'first')
    ).reset_index()
    
    log("Mesclando dados de primeiro palete...")
    estoque_df = pd.merge(
        estoque_df,
        data_primeiro_palete,
        on=['COD_ENDERECO', 'COD_ITEM'],
        how='left'
    )
    log("Data_Primeiro_Palete calculada com sucesso")
    return estoque_df
def _canon(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode("ascii")
    s = s.replace("-", " ").replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s
def ler_estoque(estoque_path):
    log(f"Lendo arquivo de estoque: {estoque_path}")

    if estoque_path.lower().endswith('.csv'):
        df = ler_csv_corretamente(estoque_path)  
    else:
        log("Lendo arquivo Excel de estoque...")
        df = pd.read_excel(estoque_path)

    df.columns = [col.strip().upper() for col in df.columns]
    log(f"Colunas do estoque: {list(df.columns)}")
    
    col_map = {
        'BLOCO': ['BLOCO', 'BLOCO_ENDERECO', 'LOCAL'],
        'COD_ENDERECO': ['COD_ENDERECO', 'ENDERECO', 'POSICAO'],
        'COD_ITEM': ['COD_ITEM', 'SKU', 'ITEM', 'PRODUTO'],
        'DATA_VALIDADE': ['DATA_VALIDADE', 'VALIDADE', 'DT_VALIDADE'],
        'CHAVE_PALLET': ['CHAVE_PALLET', 'PALLET', 'LOTE'],
        'DATA_ULTIMA_TRANSACAO': ['DATA_ULTIMA_TRANSACAO', 'DT_ULTIMA_TRANSACAO', 'ULTIMA_TRANSACAO']
    }
    try:
        col_tipo = _pick_col(df, ['TIPO_ENDERECO','TIPO ENDERECO','TIPO','TIPO_POSICAO'])
        if col_tipo != 'TIPO_ENDERECO':
            df.rename(columns={col_tipo: 'TIPO_ENDERECO'}, inplace=True)
    except ValueError:
        log("AVISO: coluna TIPO_ENDERECO não encontrada no estoque; não será aplicado filtro de tipo.")
        col_tipo = None

    if col_tipo:
        ALVOS = {"DINAMICO", "PUSH BACK", "PUSHBACK"}  
        df['TIPO_ENDERECO_CANON'] = df['TIPO_ENDERECO'].map(_canon)
        antes = len(df)
        df = df[df['TIPO_ENDERECO_CANON'].isin(ALVOS)].copy()
        df.drop(columns=['TIPO_ENDERECO_CANON'], inplace=True)
        log(f"Filtro de TIPO_ENDERECO aplicado (DINAMICO/PUSH BACK): {antes} -> {len(df)} linhas")

    for new_col, old_cols in col_map.items():
        try:
            old_col = _pick_col(df, old_cols)
            if old_col != new_col:
                df.rename(columns={old_col: new_col}, inplace=True)
                log(f"Coluna renomeada: {old_col} -> {new_col}")
        except ValueError as e:
            log(f"AVISO: {e}")
    
    colunas_necessarias = ['BLOCO', 'COD_ENDERECO', 'COD_ITEM', 'DATA_VALIDADE', 'CHAVE_PALLET']
    colunas_faltantes = [col for col in colunas_necessarias if col not in df.columns]
    
    if colunas_faltantes:
        log(f"COLUNAS FALTANTES NO ESTOQUE: {colunas_faltantes}")
        return None
    
    log("Arquivo de estoque processado com sucesso")
    return df

def ler_enderecos(enderecos_path):
    log(f"Lendo arquivo de endereços: {enderecos_path}")
    
    if enderecos_path.lower().endswith('.csv'):
        df = ler_csv_corretamente(enderecos_path)
    else:
        log("Lendo arquivo Excel de endereços...")
        df = pd.read_excel(enderecos_path)
    
    df.columns = [col.strip().upper() for col in df.columns]
    log(f"Colunas dos endereços: {list(df.columns)}")
    
    col_map = {
        'BLOCO': ['BLOCO', 'BLOCO_ENDERECO', 'LOCAL'],
        'COD_ENDERECO': ['COD_ENDERECO', 'ENDERECO', 'POSICAO'],
        'CAPACIDADE': ['CAPACIDADE', 'CAP', 'QTD_MAXIMA']
    }
    
    for new_col, old_cols in col_map.items():
        try:
            old_col = _pick_col(df, old_cols)
            if old_col != new_col:
                df.rename(columns={old_col: new_col}, inplace=True)
                log(f"Coluna renomeada: {old_col} -> {new_col}")
        except ValueError as e:
            log(f"AVISO: {e}")
    
    colunas_necessarias = ['BLOCO', 'COD_ENDERECO', 'CAPACIDADE']
    colunas_faltantes = [col for col in colunas_necessarias if col not in df.columns]
    
    if colunas_faltantes:
        log(f"COLUNAS FALTANTES NOS ENDEREÇOS: {colunas_faltantes}")
        return None
    
    log("Arquivo de endereços processado com sucesso")
    return df

def criar_estoque_posicao(estoque_df, enderecos_df):
    log("Criando tabela Estoque_Posicao...")
    estoque_df['DATA_VALIDADE'] = pd.to_datetime(
    estoque_df['DATA_VALIDADE'], 
    dayfirst=True,
    errors='coerce'
    )
    grupo_cols = ['BLOCO', 'COD_ENDERECO', 'COD_ITEM', 'DATA_VALIDADE']
    
    log("Agrupando dados de estoque...")
    estoque_agrupado = estoque_df.groupby(grupo_cols).agg(
        OCUP_PALLETS=('CHAVE_PALLET', 'nunique')
    ).reset_index()
    
    estoque_agrupado.rename(columns={'OCUP_PALLETS': 'OCUPACAO'}, inplace=True)
    log(f"Estoque agrupado - {len(estoque_agrupado)} linhas")
    
    log("Mesclando com dados de capacidade...")
    estoque_posicao = pd.merge(
        estoque_agrupado,
        enderecos_df[['BLOCO', 'COD_ENDERECO', 'CAPACIDADE']],
        on=['BLOCO', 'COD_ENDERECO'],
        how='left'
    )
    
    log("Calculando espaços livres...")
    estoque_posicao['LIVRE'] = estoque_posicao['CAPACIDADE'] - estoque_posicao['OCUPACAO']
    estoque_posicao['FLAG_CHEIO'] = (estoque_posicao['OCUPACAO'] >= estoque_posicao['CAPACIDADE']).astype(int)
    
    log("Criando chaves SKU_DATA e POS...")
    estoque_posicao['CHAVE_SKU_DATA'] = (
        estoque_posicao['COD_ITEM'].astype(str) + "|" + 
        pd.to_datetime(estoque_posicao['DATA_VALIDADE']).dt.strftime('%Y%m%d')
    )
    
    estoque_posicao['CHAVE_POS'] = (
        estoque_posicao['BLOCO'].astype(str) + "|" +
        estoque_posicao['COD_ENDERECO'].astype(str) + "|" +
        estoque_posicao['COD_ITEM'].astype(str) + "|" +
        pd.to_datetime(estoque_posicao['DATA_VALIDADE']).dt.strftime('%Y%m%d')
    )
    
    log(f"Tabela Estoque_Posicao criada com {len(estoque_posicao)} linhas")
    return estoque_posicao

def calcular_is_front(estoque_posicao, estoque_df):
    log("Calculando IsFront_RuaSKU...")
    estoque_posicao['DATA_VALIDADE'] = pd.to_datetime(estoque_posicao['DATA_VALIDADE'], dayfirst=True)
    estoque_df['DATA_PRIMEIRO_PALLET'] = pd.to_datetime(estoque_df['DATA_PRIMEIRO_PALLET'], dayfirst=True)
    
    log("Buscando data do primeiro palete...")
    primeiro_palete = estoque_df.groupby(['COD_ENDERECO', 'COD_ITEM']).agg(
        DATA_PRIMEIRO_PALLET=('DATA_PRIMEIRO_PALLET', 'min')
    ).reset_index()
    
    log("Mesclando dados do primeiro palete...")
    estoque_posicao = pd.merge(
        estoque_posicao,
        primeiro_palete,
        on=['COD_ENDERECO', 'COD_ITEM'],
        how='left'
    )
    
    log("Calculando flag IsFront_RuaSKU...")
    estoque_posicao['ISFRONT_RUASKU'] = (
        (estoque_posicao['DATA_PRIMEIRO_PALLET'].notna()) &
        (pd.to_datetime(estoque_posicao['DATA_PRIMEIRO_PALLET']) == 
         pd.to_datetime(estoque_posicao['DATA_VALIDADE']))
    ).astype(int)
    
    log("Cálculo IsFront_RuaSKU concluído")
    return estoque_posicao

def calcular_taxa_ocupacao(estoque_posicao):
    log("Calculando taxa de ocupação...")
    estoque_posicao['TAXA_OCUPACAO_ORDENACAO'] = (
        estoque_posicao['OCUPACAO'] / estoque_posicao['CAPACIDADE'].replace(0, 1)
    ).fillna(0)
    log("Taxa de ocupação calculada")
    return estoque_posicao

def habilitar_flags(estoque_posicao: pd.DataFrame) -> pd.DataFrame:

    req = ["BLOCO", "DATA_VALIDADE", "DATA_PRIMEIRO_PALLET", "ISFRONT_RUASKU"]
    faltam = [c for c in req if c not in estoque_posicao.columns]
    if faltam:
        raise KeyError(f"[FLAGS] Faltam colunas em estoque_posicao: {faltam}")

    estoque_posicao["DATA_VALIDADE"] = pd.to_datetime(estoque_posicao["DATA_VALIDADE"], errors="coerce")
    estoque_posicao["DATA_PRIMEIRO_PALLET"] = pd.to_datetime(estoque_posicao["DATA_PRIMEIRO_PALLET"], errors="coerce")

    delta = (estoque_posicao["DATA_VALIDADE"] - estoque_posicao["DATA_PRIMEIRO_PALLET"]).abs()
    estoque_posicao["TOLERANCIA_5_DIAS"] = delta.dt.days.le(5) & delta.notna()
    estoque_posicao["TOLERANCIA_5_DIAS"] = estoque_posicao["TOLERANCIA_5_DIAS"].astype(int)

    estoque_posicao["HABILITA_ORIGEM_FRONT"] = (estoque_posicao["ISFRONT_RUASKU"] == 1).astype(int)

    blocos_fg = estoque_posicao["BLOCO"].astype(str).str.upper().isin({"F", "G"})
    cond_fg   = blocos_fg & (estoque_posicao["ISFRONT_RUASKU"] == 1) & (estoque_posicao["TOLERANCIA_5_DIAS"] == 1)
    cond_out  = (~blocos_fg) & (estoque_posicao["ISFRONT_RUASKU"] == 1)
    estoque_posicao["HABILITA_DESTINO_FRONT"] = (cond_fg | cond_out).astype(int)
    try:
        total = len(estoque_posicao)
        h_orig = int(estoque_posicao["HABILITA_ORIGEM_FRONT"].sum())
        h_dest = int(estoque_posicao["HABILITA_DESTINO_FRONT"].sum())
        log(f"[FLAGS] Linhas: {total} | Origem=1: {h_orig} | Destino=1: {h_dest}")

        fg = int(estoque_posicao["BLOCO"].astype(str).str.upper().isin({"F","G"}).sum())

        cnt_tol_fg = int((
            (estoque_posicao["TOLERANCIA_5_DIAS"] == 1)
            & (estoque_posicao["BLOCO"].astype(str).str.upper().isin({"F","G"}))
        ).sum())

        log(f"[FLAGS] Linhas em blocos F/G: {fg} | Tolerância=1 nesses blocos: {cnt_tol_fg}")
    except Exception:
        pass

    return estoque_posicao


def calcular_ocupacao_otima_global(estoque_posicao):
    log("Calculando ocupação ótima global (com logs)…")

    req_cols = ["CHAVE_SKU_DATA","OCUPACAO","CAPACIDADE","FLAG_CHEIO",
                "HABILITA_ORIGEM_FRONT","HABILITA_DESTINO_FRONT","TAXA_OCUPACAO_ORDENACAO"]
    faltam = [c for c in req_cols if c not in estoque_posicao.columns]
    if faltam:
        raise KeyError(f"Colunas necessárias ausentes em estoque_posicao: {faltam}")

    def _rank(g):
        base = g[(g["FLAG_CHEIO"]==0) & (g["HABILITA_DESTINO_FRONT"]==1)].copy()
        base = base.sort_values("TAXA_OCUPACAO_ORDENACAO", ascending=False)
        ranks = pd.Series(index=g.index, dtype="float")
        ranks.loc[base.index] = base["TAXA_OCUPACAO_ORDENACAO"].rank(method="dense", ascending=False)
        return ranks

    estoque_posicao["INDICE_GRUPO_NF_GLOBAL"] = (
        estoque_posicao.groupby("CHAVE_SKU_DATA", group_keys=False).apply(_rank)
    )
    log(f"[RANK] linhas ranqueadas: {estoque_posicao['INDICE_GRUPO_NF_GLOBAL'].notna().sum()}")

    def _red(g):
        return pd.Series(
            g[(g["FLAG_CHEIO"]==0) & (g["HABILITA_ORIGEM_FRONT"]==1)]["OCUPACAO"].sum(),
            index=g.index
        )
    estoque_posicao["OCP_REDISTRIBUIVEL_GLOBAL"] = (
        estoque_posicao.groupby("CHAVE_SKU_DATA", group_keys=False).apply(_red)
    )
    log(f"[REDIST] soma total redistribuível: {estoque_posicao['OCP_REDISTRIBUIVEL_GLOBAL'].sum():.0f}")

    def _cap_acum(g):
        sub = g[(g["FLAG_CHEIO"]==0) & (g["HABILITA_DESTINO_FRONT"]==1)].copy()
        sub = sub.sort_values("INDICE_GRUPO_NF_GLOBAL", ascending=True)
        sub["CAP_ACUM_ANTES"] = sub["CAPACIDADE"].cumsum().shift(1).fillna(0)
        out = pd.Series(0.0, index=g.index)
        out.loc[sub.index] = sub["CAP_ACUM_ANTES"].values
        return out
    estoque_posicao["OCP_CAP_ACUMULADA_NF_ANTES_GLOBAL"] = (
        estoque_posicao.groupby("CHAVE_SKU_DATA", group_keys=False).apply(_cap_acum)
    )

    def _ocp(row):
        if row["FLAG_CHEIO"] == 1:
            return float(row["OCUPACAO"])
        cap = float(row["CAPACIDADE"] or 0)
        red = float(row["OCP_REDISTRIBUIVEL_GLOBAL"] or 0)
        cap_ac = float(row["OCP_CAP_ACUMULADA_NF_ANTES_GLOBAL"] or 0)
        return max(0.0, min(cap, red - cap_ac))

    estoque_posicao["OCP_OCUPACAO_OTIMA_GLOBAL"] = estoque_posicao.apply(_ocp, axis=1)

    neg = (estoque_posicao["OCP_OCUPACAO_OTIMA_GLOBAL"] < 0).sum()
    if neg:
        log(f"[CHECK] {neg} linhas com OCP_OTIMA negativa (depois do max(0,…)).")
    acima = (estoque_posicao["OCP_OCUPACAO_OTIMA_GLOBAL"] > estoque_posicao["CAPACIDADE"]).sum()
    if acima:
        log(f"[CHECK] {acima} linhas com OCP_OTIMA > CAPACIDADE (antes de min(cap,…)).")

    impacto = (estoque_posicao
               .assign(DIFF=lambda d: (d["OCP_OCUPACAO_OTIMA_GLOBAL"] - d["OCUPACAO"]).abs())
               .groupby("CHAVE_SKU_DATA")["DIFF"].sum()
               .sort_values(ascending=False)
               .head(20)
               .reset_index())
    _dump(impacto, "top20_grupos_impacto.csv")

    top5 = set(impacto["CHAVE_SKU_DATA"].head(5).tolist())
    det = estoque_posicao[estoque_posicao["CHAVE_SKU_DATA"].isin(top5)].copy()
    _dump(det, "detalhe_top5_grupos_otimizacao.csv")

    log("Ocupação ótima global calculada.")
    return estoque_posicao


def calcular_indicadores(estoque_posicao, enderecos_df):
    log("Calculando indicadores (com logs)…")

    total_cap = float(enderecos_df["CAPACIDADE"].sum())
    livre = float(estoque_posicao["CAPACIDADE"].sum() - estoque_posicao["OCUPACAO"].sum())
    porColmeia = (livre / total_cap) if total_cap else 0.0
    log(f"[COLMEIA] livre={livre:.0f}  cap_total={total_cap:.0f}  colmeia={porColmeia:.4f}")

    end_com_itens = set(estoque_posicao.loc[estoque_posicao["OCUPACAO"]>0,"COD_ENDERECO"].astype(str))
    todos_end = set(enderecos_df["COD_ENDERECO"].astype(str))
    end_vazios_real = len(todos_end - end_com_itens)
    log(f"[RUAS REAL] total_end={len(todos_end)}  com_itens={len(end_com_itens)}  vazias_real={end_vazios_real}")

    if "OCP_OCUPACAO_OTIMA_GLOBAL" not in estoque_posicao.columns:
        log("[ERRO] OCP_OCUPACAO_OTIMA_GLOBAL ausente. Verifique a ordem das chamadas.")
        end_vazios_otimo = end_vazios_real
    else:
        end_otim = set(estoque_posicao.loc[estoque_posicao["OCP_OCUPACAO_OTIMA_GLOBAL"]>0,"COD_ENDERECO"].astype(str))
        end_vazios_otimo = len(todos_end - end_otim)
        log(f"[RUAS OTIM] com_itens_otim={len(end_otim)}  vazias_otimo={end_vazios_otimo}")

        liberadas_por_endereco = sorted(list((todos_end - end_com_itens) ^ (todos_end - end_otim)))[:50]
        if liberadas_por_endereco:
            log(f"[DETALHE] primeiros endereços que mudaram de status (max 50): {liberadas_por_endereco}")

    if "CHAVE_POS" not in estoque_posicao.columns:
        log("[ERRO] CHAVE_POS ausente em estoque_posicao.")
        pallets_movimentados = 0
    else:
        grp = (estoque_posicao
               .groupby("CHAVE_POS")
               .agg(Ocupacao_Real=("OCUPACAO","sum"),
                    Ocupacao_Otima=("OCP_OCUPACAO_OTIMA_GLOBAL","sum"))
               .reset_index())
        grp["DIFF_ABS"] = (grp["Ocupacao_Otima"] - grp["Ocupacao_Real"]).abs()
        pallets_movimentados = grp["DIFF_ABS"].sum() / 2.0
        log(f"[MOVE] soma |Δ| = {grp['DIFF_ABS'].sum():.0f}  paletes_mov = {pallets_movimentados:.0f}")
        _dump(grp.sort_values("DIFF_ABS", ascending=False).head(50), "pallets_mov_top50_chavepos.csv")

    end_vazios_total = end_vazios_otimo - end_vazios_real
    log(f"[LIBERADAS] {end_vazios_total} ruas")

    miss_cap = estoque_posicao["CAPACIDADE"].isnull().sum()
    if miss_cap:
        top = (estoque_posicao[estoque_posicao["CAPACIDADE"].isnull()][["BLOCO","COD_ENDERECO"]]
               .drop_duplicates())
        _dump(top, "enderecos_sem_capacidade.csv")
        log(f"[CHECK] {miss_cap} linhas sem CAPACIDADE — veja enderecos_sem_capacidade.csv")

    return {
        'porColmeia': porColmeia,
        'end_vazios_real': int(end_vazios_real),
        'end_vazios_otimo': int(end_vazios_otimo),
        'pallets_movimentados': float(pallets_movimentados),
        'end_vazios_total': int(end_vazios_total)
    }


def enviar_relatorio_email(assunto, corpo):
    log("Preparando para enviar e-mail...")
    try:
        yag = yagmail.SMTP(user='mdiasbrancoautomacao@gmail.com', password='secwygmzlibyxhhh')
        yag.send(
            to=['douglas.lins2@mdiasbranco.com.br'],
            subject=assunto,
            contents=corpo
        )
        log("E-mail de resultado enviado.")
    except Exception as e:
        log(f"Falha ao enviar e-mail de resultado: {e}")

def filtrar_enderecos_por_tipo(enderecos_df: pd.DataFrame) -> pd.DataFrame:
    alvo = {"DINAMICO", "MEZANINO", "PORTA PALETE", "PORTA PALETES", "PORTA-PALETE", "PORTA-PALETES"}
    try:
        col_tipo = _pick_col(enderecos_df, ['TIPO_ENDERECO','TIPO ENDERECO','TIPO','TIPO_POSICAO'])
        if col_tipo != 'TIPO_ENDERECO':
            enderecos_df = enderecos_df.rename(columns={col_tipo: 'TIPO_ENDERECO'})
        enderecos_df['_TIPO_CANON'] = enderecos_df['TIPO_ENDERECO'].map(_canon)
        antes = len(enderecos_df)
        enderecos_df = enderecos_df[enderecos_df['_TIPO_CANON'].isin(alvo)].copy()
        enderecos_df.drop(columns=['_TIPO_CANON'], inplace=True)
        log(f"[ENDERECOS] Filtro TIPO_ENDERECO (DINAMICO/MEZANINO/PORTA PALETE): {antes} -> {len(enderecos_df)} linhas")
        vc = enderecos_df['TIPO_ENDERECO'].astype(str).str.upper().value_counts().head(10)
        log(f"[ENDERECOS] Top tipos após filtro:\n{vc.to_string()}")
    except ValueError:
        log("[ENDERECOS] Coluna TIPO_ENDERECO não encontrada — filtro não aplicado.")
    return enderecos_df

def main():
    inicio = time.time()
    log("=" * 60)
    log("INICIANDO PROCESSO DE ANÁLISE DE ESTOQUE")
    log("=" * 60)
    
    try:
        fonte_dir = encontrar_pasta_onedrive_empresa()
        if not fonte_dir:
            log("Pasta do OneDrive não encontrada! Encerrando processo.")
            return
        
        arquivos = os.listdir(fonte_dir)
        log(f"Arquivos encontrados na pasta: {arquivos}")
        
        estoque_files = [f for f in arquivos if 'estoque_detalhado' in f.lower() and f.lower().endswith(('.csv','.xlsx','.xls'))]
        enderecos_files = [f for f in arquivos if 'cap_endereco' in f.lower() and f.lower().endswith(('.csv','.xlsx','.xls'))]
        
        log(f"Arquivos de estoque encontrados: {estoque_files}")
        log(f"Arquivos de endereços encontrados: {enderecos_files}")
        
        if not estoque_files:
            log("Nenhum arquivo de estoque encontrado! Encerrando processo.")
            return
            
        if not enderecos_files:
            log("Nenhum arquivo de endereços encontrado! Encerrando processo.")
            return
        
        estoque_path = os.path.join(fonte_dir, estoque_files[0])
        enderecos_path = os.path.join(fonte_dir, enderecos_files[0])
        
        estoque_df = ler_estoque(estoque_path)
        enderecos_df = ler_enderecos(enderecos_path)
        enderecos_df = filtrar_enderecos_por_tipo(enderecos_df)
        estoque_df = verificar_qualidade_dados(estoque_df, "estoque")
        enderecos_df = verificar_qualidade_dados(enderecos_df, "endereços")

        if estoque_df is None or enderecos_df is None:
            log("Falha ao processar arquivos! Encerrando processo.")
            return
        estoque_df = calcular_data_primeiro_palete(estoque_df)
        estoque_posicao = criar_estoque_posicao(estoque_df, enderecos_df)
        estoque_posicao = calcular_is_front(estoque_posicao, estoque_df)
        estoque_posicao = calcular_taxa_ocupacao(estoque_posicao)
        estoque_posicao = habilitar_flags(estoque_posicao) 
        estoque_posicao = calcular_ocupacao_otima_global(estoque_posicao)

        indicadores = calcular_indicadores(estoque_posicao, enderecos_df)
        salvar_estoque_posicao_no_diretorio(fonte_dir, estoque_posicao, salvar_xlsx=True)  

        
        hora_atual = datetime.now().hour
        if hora_atual < 6:
            turno = "Madrugada"
        elif hora_atual < 14:
            turno = "Manhã"
        elif hora_atual < 22:
            turno = "Tarde"
        else:
            turno = "Noite"
        
        assunto = f"[Auditoria 24x7] Relatório de Estoque - Turno {turno}"
        
        corpo = f"""
        <h2>Relatório de Otimização de Estoque</h2>
        <p>Turno: {turno}</p>
        <p>Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
        
        <h3>Indicadores Calculados:</h3>
        <ul>
            <li><strong>Colmeia (Espaço Livre):</strong> {indicadores['porColmeia']:.2%}</li>
            <li><strong>Ruas Vazias (Real):</strong> {indicadores['end_vazios_real']}</li>
            <li><strong>Ruas Vazias (Otimizado):</strong> {indicadores['end_vazios_otimo']}</li>
            <li><strong>Paletes Movimentados:</strong> {indicadores['pallets_movimentados']}</li>
            <li><strong>Ruas Liberadas:</strong> {indicadores['end_vazios_total']}</li>
        </ul>
        
        <p>Este é um relatório automático gerado pelo sistema de otimização de estoque.</p>
        """
        
        enviar_relatorio_email(assunto, corpo)
        
        tempo_execucao = time.time() - inicio
        log(f"Processo concluído com sucesso em {tempo_execucao:.2f} segundos!")
        
    except Exception as e:
        log(f"ERRO NO PROCESSO PRINCIPAL: {e}")
        enviar_relatorio_email(
            assunto="[Auditoria 24x7] ERRO no processo de estoque",
            corpo=f"Ocorreu um erro durante o processamento do estoque: {str(e)}"
        )

if __name__ == "__main__":
    main()