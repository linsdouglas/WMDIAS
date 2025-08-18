import pandas as pd
import os
from datetime import datetime

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")
def filtrar_chaves_mes(df, coluna_chave):
    chaves = df[coluna_chave].astype(str).str.strip()
    mascara = chaves.str.startswith('M431') & chaves.str.endswith('M')
    df_filtrado = df[mascara].copy()
    
    log(f"\n[FILTRO] Total de chaves antes do filtro: {len(df)}")
    log(f"[FILTRO] Chaves MES encontradas: {len(df_filtrado)}")
    log(f"[FILTRO] Exemplos de chaves MES: {df_filtrado[coluna_chave].head(3).tolist()}")
    
    return df_filtrado
def ler_csv_corretamente(csv_path):
    """Lê o CSV corrigindo o problema de deslocamento de colunas"""
    log(f"Processando arquivo: {os.path.basename(csv_path)}")
    
    with open(csv_path, 'r', encoding='latin1') as f:
        lines = [line.strip().split(';') for line in f.readlines() if line.strip()]
    
    if not lines:
        raise ValueError("Arquivo CSV vazio ou inválido")
    
    header = lines[0]
    data = []
    problemas = 0
    
    for i, line in enumerate(lines[1:]):
        if len(line) == len(header):
            data.append(line)
        else:
            problemas += 1
            corrected = line[:len(header)]
            if len(corrected) == len(header):
                data.append(corrected)
            else:
                log(f"[AVISO] Linha {i+2} ignorada - número inválido de colunas: {len(line)} (esperado: {len(header)})")
    
    if problemas > 0:
        log(f"[AVISO] Foram corrigidas {problemas} linhas com problemas de formatação")
    
    df = pd.DataFrame(data, columns=header)
    
    log("\n[DEBUG] Estrutura do DataFrame carregado:")
    log(f"Total de linhas: {len(df)}")
    log(f"Colunas: {list(df.columns)}")
    log("\n[DEBUG] Primeiras linhas dos dados:")
    for i, row in df.head(3).iterrows():
        log(f"Linha {i}: {row.to_dict()}")
    
    return df

def _pick_col(df, candidatos):
    cols = {c.strip().lower(): c for c in df.columns}
    for cand in candidatos:
        k = cand.strip().lower()
        if k in cols:
            return cols[k]
    norm_cands = {cand.strip().lower().replace(" ", "_") for cand in candidatos}
    for c in df.columns:
        if c.strip().lower().replace(" ", "_") in norm_cands:
            return c
    return None

def _preparar_ultimos_movimentos(df_hist):
    col_chave = _pick_col(df_hist, ["CHAVE_PALLET"])
    col_created = _pick_col(df_hist, ["CREATED_AT"])
    col_tipo = _pick_col(df_hist, ["TIPO_MOVIMENTO"])
    col_motivo = _pick_col(df_hist, ["MOTIVO"])

    if col_chave is None or col_created is None:
        raise ValueError("Não encontrei colunas de chave ou CREATED_AT no histórico após leitura.")

    df = df_hist.copy()
    df[col_chave] = df[col_chave].astype(str).str.strip()
    df[col_created] = pd.to_datetime(df[col_created], errors="coerce", dayfirst=True, utc=False, infer_datetime_format=False)
    df = df.dropna(subset=[col_created])

    df = df.sort_values([col_chave, col_created])
    last = df.groupby(col_chave, as_index=False).tail(1)

    last = last[[col_chave, col_created] + ([col_tipo] if col_tipo else []) + ([col_motivo] if col_motivo else [])].copy()
    last = last.rename(columns={
        col_chave: "chave_pallete",
        col_created: "created_at_ultimo",
        **({col_tipo: "TIPO_MOVIMENTO_ULTIMO"} if col_tipo else {}),
        **({col_motivo: "MOTIVO_ULTIMO"} if col_motivo else {})
    })

    if "TIPO_MOVIMENTO_ULTIMO" not in last.columns:
        last["TIPO_MOVIMENTO_ULTIMO"] = pd.NA
    if "MOTIVO_ULTIMO" not in last.columns:
        last["MOTIVO_ULTIMO"] = pd.NA

    df["__tem_remessa_saida__"] = (
        df[col_motivo].astype(str).str.strip().str.upper().eq("REMESSA") if col_motivo else False
    ) & (
        df[col_tipo].astype(str).str.strip().str.upper().eq("SAIDA") if col_tipo else False
    )
    flags = df.groupby(col_chave)["__tem_remessa_saida__"].any().reset_index()
    flags = flags.rename(columns={col_chave: "chave_pallete", "__tem_remessa_saida__": "tem_remessa_saida"})

    out = last.merge(flags, on="chave_pallete", how="left")
    out["tem_remessa_saida"] = out["tem_remessa_saida"].fillna(False)

    return out

def analisar_rastreabilidade(fonte_dir):
    log("Localizando arquivos na pasta...")
    arquivos = os.listdir(fonte_dir)

    rastreabilidade_files = [f for f in arquivos if 'rastreabilidade' in f.lower() and f.endswith('.csv')]
    if not rastreabilidade_files:
        raise FileNotFoundError("Nenhum arquivo de rastreabilidade encontrado")
    rastreabilidade_path = os.path.join(fonte_dir, max(rastreabilidade_files, key=lambda x: os.path.getmtime(os.path.join(fonte_dir, x))))

    historico_files = [f for f in arquivos if 'historico_transacoes' in f.lower() and f.endswith('.csv')]
    if not historico_files:
        raise FileNotFoundError("Nenhum arquivo de histórico de transações encontrado")
    historico_path = os.path.join(fonte_dir, max(historico_files, key=lambda x: os.path.getmtime(os.path.join(fonte_dir, x))))

    auditoria_path = os.path.join(fonte_dir, "auditoria_24_7.xlsx")

    log("\nCarregando arquivo de rastreabilidade...")
    df_rastreabilidade = ler_csv_corretamente(rastreabilidade_path)

    log("\nCarregando arquivo de histórico de transações...")
    df_historico = ler_csv_corretamente(historico_path)

    log("\nAplicando filtro para chaves MES...")
    coluna_rastreio_mes = 'COD_RASTREABILIDADE'
    df_rastreabilidade = filtrar_chaves_mes(df_rastreabilidade, coluna_rastreio_mes)

    coluna_pallet_mes = 'CHAVE_PALLET'
    df_historico = filtrar_chaves_mes(df_historico, coluna_pallet_mes)

    coluna_rastreio = None
    for col in df_rastreabilidade.columns:
        if 'COD_RASTREABILIDADE' in col.upper():
            coluna_rastreio = col
            break
    if not coluna_rastreio:
        raise ValueError("Coluna 'COD_RASTREABILIDADE' não encontrada no arquivo de rastreabilidade")

    coluna_pallet = None
    for col in df_historico.columns:
        if 'CHAVE_PALLET' in col.upper() or 'CHAVE_PALLETE' in col.upper():
            coluna_pallet = col
            break
    if not coluna_pallet:
        raise ValueError("Coluna 'CHAVE_PALLET' não encontrada no arquivo de histórico")

    log("\nCalculando última movimentação por chave e flags de REMESSA/SAÍDA...")
    df_ultimos = _preparar_ultimos_movimentos(df_historico)

    df_auditoria = pd.DataFrame(columns=[
        'chave_pallete',
        'status',
        'created_at_ultimo',
        'TIPO_MOVIMENTO_ULTIMO',
        'MOTIVO_ULTIMO'
    ])

    codigos_rastreabilidade = [str(codigo).strip()
                               for codigo in df_rastreabilidade[coluna_rastreio].unique()
                               if pd.notna(codigo) and str(codigo).strip()]

    log(f"\nIniciando análise de {len(codigos_rastreabilidade)} códigos de rastreabilidade...")

    mapa_ultimos = df_ultimos.set_index("chave_pallete").to_dict(orient="index")

    for codigo in codigos_rastreabilidade:
        log(f"\n[DEBUG] Analisando código: {codigo}")

        info = mapa_ultimos.get(codigo)
        if info is None:
            log("[DEBUG] Nenhuma movimentação encontrada no histórico para esta chave")
            df_auditoria = pd.concat([df_auditoria, pd.DataFrame([{
                'chave_pallete': codigo,
                'status': 'NÃO ENCONTRADO MOVIMENTAÇÃO',
                'created_at_ultimo': pd.NaT,
                'TIPO_MOVIMENTO_ULTIMO': pd.NA,
                'MOTIVO_ULTIMO': pd.NA
            }])], ignore_index=True)
            continue

        tem_remessa_saida = bool(info.get("tem_remessa_saida", False))
        status = 'OK - REMESSA E SAÍDA ENCONTRADAS' if tem_remessa_saida else 'MOVIMENTAÇÃO ENCONTRADA MAS SEM REMESSA/SAÍDA'

        df_auditoria = pd.concat([df_auditoria, pd.DataFrame([{
            'chave_pallete': codigo,
            'status': status,
            'created_at_ultimo': info.get('created_at_ultimo'),
            'TIPO_MOVIMENTO_ULTIMO': info.get('TIPO_MOVIMENTO_ULTIMO'),
            'MOTIVO_ULTIMO': info.get('MOTIVO_ULTIMO')
        }])], ignore_index=True)

    log(f"\nSalvando resultados da auditoria em {auditoria_path}")
    df_auditoria.to_excel(auditoria_path, index=False)

    total = len(df_auditoria)
    nao_encontrados = len(df_auditoria[df_auditoria['status'] == 'NÃO ENCONTRADO MOVIMENTAÇÃO'])
    encontrados_sem = len(df_auditoria[df_auditoria['status'] == 'MOVIMENTAÇÃO ENCONTRADA MAS SEM REMESSA/SAÍDA'])
    encontrados_com = len(df_auditoria[df_auditoria['status'] == 'OK - REMESSA E SAÍDA ENCONTRADAS'])

    log("\n=== RESUMO DA AUDITORIA ===")
    log(f"Total de pallets analisados: {total}")
    log(f"Pallets sem movimentação: {nao_encontrados} ({nao_encontrados/total:.1%})")
    log(f"Pallets com movimentação mas sem REMESSA/SAÍDA: {encontrados_sem} ({encontrados_sem/total:.1%})")
    log(f"Pallets com REMESSA e SAÍDA encontradas: {encontrados_com} ({encontrados_com/total:.1%})")

    return df_auditoria


if __name__ == "__main__":
    def encontrar_pasta_onedrive_empresa():
        user_dir = os.environ["USERPROFILE"]
        possiveis = os.listdir(user_dir)
        for nome in possiveis:
            if "DIAS BRANCO" in nome.upper():
                caminho_completo = os.path.join(user_dir, nome)
                if os.path.isdir(caminho_completo) and "Gestão de Estoque - Gestão_Auditoria" in os.listdir(caminho_completo):
                    return os.path.join(caminho_completo, "Gestão de Estoque - Gestão_Auditoria")
        return None
    
    fonte_dir = encontrar_pasta_onedrive_empresa()
    
    if not fonte_dir:
        log("Pasta OneDrive não encontrada")
        exit(1)
    
    try:
        df_auditoria = analisar_rastreabilidade(fonte_dir)
        log("Análise concluída com sucesso!")
    except Exception as e:
        log(f"Erro durante a análise: {str(e)}")
        raise