import pandas as pd
import numpy as np
import os
import yagmail
from datetime import datetime, timedelta
import logging
import time
import csv
import unicodedata, re
import glob
import schedule
from threading import Thread
import traceback
import sys

DEBUG_DIR = os.path.join(os.getcwd(), f"_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

def setup_logging():
    """Configura logging unificado"""
    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(log_dir, f"auditoria_estoque_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def log(mensagem):
    logger.info(mensagem)

def _dump(df, name, index=False):
    path = os.path.join(DEBUG_DIR, name)
    try:
        df.to_csv(path, sep=";", index=index, encoding="utf-8-sig")
        log(f"[DEBUG] Exportado: {path} ({len(df)} linhas)")
    except Exception as e:
        log(f"[WARN] Falha ao exportar {name}: {e}")
def _find_onedrive_subfolder(subfolder_name: str):
    """Encontra pasta do OneDrive"""
    user_dir = os.environ.get("USERPROFILE", "")
    for nome in os.listdir(user_dir):
        if "DIAS BRANCO" in nome.upper():
            raiz = os.path.join(user_dir, nome)
            if os.path.isdir(raiz) and subfolder_name in os.listdir(raiz):
                return os.path.join(raiz, subfolder_name)
    return None

def encontrar_pasta_onedrive_empresa():
    return _find_onedrive_subfolder("Gestão de Estoque - Gestão_Auditoria")

transacoes_analisadas = set()

EXPECTED_COLS_TRANSACOES = [
    "ID", "COD_CENTRO", "LOCAL_EXPEDICAO", "COD_DEPOSITO", "COD_ENDERECO", 
    "COD_ITEM", "DESC_ITEM", "LOTE", "VOLUME", "UOM", 
    "DATA_VALIDADE", "TIPO_MOVIMENTO", "CHAVE_PALLET", "MOTIVO", "CREATED_AT", "CRIADO_POR_LOGIN"
]

def _split_fix(parts, n):
    if len(parts) > n:
        head = parts[:n-1]
        tail = ";".join(parts[n-1:])
        return head + [tail]
    elif len(parts) < n:
        return parts + [""] * (n - len(parts))
    return parts

def _read_csv_transacoes_strict(path, encodings=("utf-8-sig", "utf-8", "latin1", "cp1252")):
    last_exc = None
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                lines = [ln.rstrip("\r\n") for ln in f if ln.strip()]

            log(f"Total de linhas lidas: {len(lines)}")

            header_idx = None
            for i, ln in enumerate(lines):
                parts = ln.split(";")
                if ("ID" in parts) and ("COD_ITEM" in parts) and ("TIPO_MOVIMENTO" in parts):
                    header_idx = i
                    break

            if header_idx is None:
                header_idx = 0
                log("Header não encontrado, usando primeira linha")

            header = _split_fix(lines[header_idx].split(";"), len(EXPECTED_COLS_TRANSACOES))

            rows = []
            for ln in lines[header_idx+1:]:
                parts = _split_fix(ln.split(";"), len(EXPECTED_COLS_TRANSACOES))
                rows.append(parts)

            df_out = pd.DataFrame(rows, columns=EXPECTED_COLS_TRANSACOES, dtype=str)
            df_out.columns = [c.strip().upper() for c in df_out.columns]

            for c in df_out.columns:
                df_out[c] = (
                    df_out[c].astype(str)
                             .str.strip()
                             .str.replace('"', '', regex=False)
                             .str.replace("'",  '', regex=False)
                )

            log(f"CSV lido com sucesso usando encoding: {enc}")
            return df_out

        except Exception as e:
            log(f"Falha com encoding {enc}: {e}")
            last_exc = e
            continue

    raise last_exc if last_exc else RuntimeError("Falha ao ler CSV de transações com encodings testados.")


def encontrar_arquivo_mais_recente(base_dir, padrao):
    try:
        arquivos = glob.glob(os.path.join(base_dir, padrao))
        if arquivos:
            arquivo_mais_recente = max(arquivos, key=os.path.getmtime)
            log(f"Arquivo encontrado: {os.path.basename(arquivo_mais_recente)}")
            return arquivo_mais_recente
        return None
    except Exception as e:
        log(f"Erro ao buscar arquivo {padrao}: {e}")
        return None

def encontrar_arquivo_transacoes(base_dir):
    log(f"Procurando arquivos de transações em: {base_dir}")
    
    padroes = ['*historico_transacoes*', '*transacoes*', '*movimentacoes*', '*historico*']
    extensoes = ['.csv', '.xlsx', '.xls']
    
    for padrao in padroes:
        for extensao in extensoes:
            arquivo = encontrar_arquivo_mais_recente(base_dir, f"{padrao}{extensao}")
            if arquivo:
                return arquivo
    
    log("Nenhum arquivo de transações encontrado")
    return None

def encontrar_arquivo_estoque_posicao(base_dir):
    log(f"Procurando arquivo estoque_posicao em: {base_dir}")
    
    padroes = ['estoque_posicao_*.csv', 'estoque_posicao_*.xlsx']
    
    for padrao in padroes:
        arquivo = encontrar_arquivo_mais_recente(base_dir, padrao)
        if arquivo:
            return arquivo
    
    log("Nenhum arquivo estoque_posicao encontrado")
    return None

def ler_estoque_posicao(caminho_estoque):
    log(f"Lendo estoque_posicao de: {caminho_estoque}")
    
    try:
        if caminho_estoque.lower().endswith('.csv'):
            df = pd.read_csv(caminho_estoque, sep=';', encoding='utf-8-sig', low_memory=False)
        else:
            df = pd.read_excel(caminho_estoque)
        
        log(f"Estoque_posicao lido - Shape: {df.shape}")
        
        letras_validas = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                           'I13','I14', 'J', 'K', 'Q', 'R17', 'S', 'T', 'U', 'V']
        mascara_enderecos_validos = df['COD_ENDERECO'].str.upper().str[0].isin(letras_validas)
        df = df[mascara_enderecos_validos].copy()
        
        log(f"Estoque_posicao após filtro de endereços - Shape: {df.shape}")
        
        colunas_data = ['DATA_VALIDADE', 'DATA_PRIMEIRO_PALLET']
        for col in colunas_data:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
    except Exception as e:
        log(f"Erro ao ler estoque_posicao: {e}")
        return None

def ler_transacoes(caminho_transacoes):
    log(f"Lendo transações com método robusto de: {caminho_transacoes}")

    try:
        if caminho_transacoes.lower().endswith('.csv'):
            transacoes_df = _read_csv_transacoes_strict(caminho_transacoes)
        else:
            transacoes_df = pd.read_excel(caminho_transacoes)
            transacoes_df.columns = [col.strip().upper() for col in transacoes_df.columns]

        log(f"Transações lidas - Shape: {transacoes_df.shape}")

        if 'VOLUME' in transacoes_df.columns:
            transacoes_df['VOLUME'] = (
                transacoes_df['VOLUME'].astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(r"[^0-9\.\-]", "", regex=True)
            )
            transacoes_df['VOLUME'] = pd.to_numeric(transacoes_df['VOLUME'], errors="coerce")

        for col in ('DATA_VALIDADE', 'CREATED_AT'):
            if col in transacoes_df.columns:
                transacoes_df[col] = pd.to_datetime(transacoes_df[col], dayfirst=True, errors="coerce")

        return transacoes_df

    except Exception as e:
        log(f"Erro ao ler transações: {e}")
        return None


def filtrar_transacoes_recentes(transacoes_df, horas_retroativas=1):
    log(f"Filtrando transações das últimas {horas_retroativas} hora(s) do dia atual")
    
    try:
        if 'CREATED_AT' not in transacoes_df.columns:
            log("Coluna CREATED_AT não encontrada - usando todas as transações")
            return transacoes_df

        datas_validas = transacoes_df['CREATED_AT'].notnull()
        if datas_validas.sum() == 0:
            log("Nenhuma data válida encontrada em CREATED_AT - usando todas as transações")
            return transacoes_df
        
        if not pd.api.types.is_datetime64_any_dtype(transacoes_df['CREATED_AT']):
            transacoes_df['CREATED_AT'] = pd.to_datetime(transacoes_df['CREATED_AT'], errors='coerce')
        
        agora = datetime.now()
        data_limite = agora - timedelta(hours=horas_retroativas)
        data_hoje = agora.replace(hour=0, minute=0, second=0, microsecond=0)
        
        log(f"Agora: {agora}")
        log(f"Data limite: {data_limite}")
        log(f"Data hoje (início do dia): {data_hoje}")
        
        mascara_recentes = (
            (transacoes_df['CREATED_AT'] >= data_limite) & 
            (transacoes_df['CREATED_AT'] <= agora) &
            (transacoes_df['CREATED_AT'] >= data_hoje)
        )
        
        transacoes_recentes = transacoes_df[mascara_recentes].copy()
        
        log(f"Transações das últimas {horas_retroativas} hora(s) do dia atual: {len(transacoes_recentes)}")
        
        if len(transacoes_recentes) > 0:
            data_min = transacoes_recentes['CREATED_AT'].min()
            data_max = transacoes_recentes['CREATED_AT'].max()
            log(f"Range de datas recentes: {data_min} até {data_max}")
            
        return transacoes_recentes
            
    except Exception as e:
        log(f"Erro ao filtrar transações: {e}")
        return transacoes_df

def filtrar_enderecos_validos(transacoes_df):
    log("Filtrando endereços válidos (A-K, Q-V)")
    
    letras_validas = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                        'I13','I14', 'J', 'K', 'Q', 'R17', 'S', 'T', 'U', 'V']
    mascara_enderecos_validos = transacoes_df['COD_ENDERECO'].str.upper().str[0].isin(letras_validas)
    transacoes_filtradas = transacoes_df[mascara_enderecos_validos].copy()
    
    log(f"Total de transações antes do filtro: {len(transacoes_df)}")
    log(f"Total de transações após filtro de endereços: {len(transacoes_filtradas)}")
    
    return transacoes_filtradas

def analisar_risco_colmeia(transacoes_df, estoque_posicao):
    log("=== INICIANDO ANÁLISE DE RISCO DE COLMEIA ===")
    
    alertas = []
    global transacoes_analisadas
    
    transacoes_df = filtrar_enderecos_validos(transacoes_df)
    saidas = transacoes_df[transacoes_df['TIPO_MOVIMENTO'].str.upper() == 'SAIDA']
    log(f"Total de saídas encontradas: {len(saidas)}")
    
    if len(saidas) == 0:
        log("Nenhuma saída encontrada para análise")
        return alertas
    
    saidas_novas = saidas[~saidas['ID'].astype(str).isin(transacoes_analisadas)]
    log(f"Novas saídas para análise: {len(saidas_novas)}")
    
    if len(saidas_novas) == 0:
        log("Todas as saídas já foram analisadas anteriormente")
        return alertas
    
    estoque_dict = {}
    for _, row in estoque_posicao.iterrows():
        chave = f"{row['COD_ENDERECO']}|{row['COD_ITEM']}|{row['DATA_VALIDADE'].strftime('%Y%m%d')}"
        estoque_dict[chave] = {
            'OCUPACAO': row['OCUPACAO'],
            'CAPACIDADE': row['CAPACIDADE'],
            'LIVRE': row['LIVRE'],
            'FLAG_CHEIO': row.get('FLAG_CHEIO', 0)
        }
    
    for idx, transacao in saidas_novas.iterrows():
        try:
            transacao_id = str(transacao.get('ID', 'N/A'))
            if transacao_id in transacoes_analisadas:
                continue
                
            sku = transacao['COD_ITEM']
            data_validade = transacao['DATA_VALIDADE']
            volume = transacao['VOLUME']
            endereco_origem = transacao['COD_ENDERECO']
            
            if pd.isna(data_validade):
                continue
            
            chave_origem = f"{endereco_origem}|{sku}|{data_validade.strftime('%Y%m%d')}"
            info_origem = estoque_dict.get(chave_origem)
            
            if not info_origem:
                continue
            
            estava_cheio_antes = info_origem['FLAG_CHEIO'] == 1
            
            if not estava_cheio_antes:
                continue
            
            chave_sku_data = f"{sku}|{data_validade.strftime('%Y%m%d')}"
            outros_enderecos = estoque_posicao[
                (estoque_posicao['CHAVE_SKU_DATA'] == chave_sku_data) &
                (estoque_posicao['COD_ENDERECO'] != endereco_origem)
            ]
            
            if len(outros_enderecos) == 0:
                continue
            
            enderecos_com_capacidade = outros_enderecos[outros_enderecos['LIVRE'] > 0]
            
            if len(enderecos_com_capacidade) > 0:
                alerta = {
                    'ID_TRANSACAO': transacao_id,
                    'DATA_TRANSACAO': transacao.get('CREATED_AT', 'N/A'),
                    'SKU': sku,
                    'DESC_ITEM': transacao.get('DESC_ITEM', 'N/A'),
                    'LOTE': transacao.get('LOTE', 'N/A'),
                    'DATA_VALIDADE': data_validade,
                    'VOLUME_MOVIMENTADO': volume,
                    'ENDERECO_ORIGEM': endereco_origem,
                    'TIPO_MOVIMENTO': transacao['TIPO_MOVIMENTO'],
                    'MOTIVO': transacao.get('MOTIVO', 'N/A'),
                    'ESTAVA_CHEIO': True,
                    'OCUPACAO_ORIGEM': info_origem['OCUPACAO'],
                    'CAPACIDADE_ORIGEM': info_origem['CAPACIDADE'],
                    'OPORTUNIDADES_ENCONTRADAS': len(enderecos_com_capacidade),
                    'DETALHES_OPORTUNIDADES': []
                }
                
                for _, oportunidade in enderecos_com_capacidade.head(3).iterrows():
                    alerta['DETALHES_OPORTUNIDADES'].append({
                        'ENDERECO_ALTERNATIVO': oportunidade['COD_ENDERECO'],
                        'BLOCO': oportunidade['BLOCO'],
                        'OCUPACAO_ATUAL': oportunidade['OCUPACAO'],
                        'CAPACIDADE': oportunidade['CAPACIDADE'],
                        'LIVRE': oportunidade['LIVRE']
                    })
                
                alertas.append(alerta)
                transacoes_analisadas.add(transacao_id)
                log(f"Alerta de colmeia encontrado: {endereco_origem} -> SKU: {sku}")
                
        except Exception as e:
            log(f"Erro ao processar transação: {e}")
            continue
    
    log(f"Total de alertas de colmeia encontrados: {len(alertas)}")
    return alertas

def _alert_key_for_excel(alerta):
    rid = str(alerta.get('ID_Transacao') or alerta.get('ID_TRANSACAO') or '').strip()
    if rid and rid.upper() != 'N/A':
        return f"ID:{rid}"
    parts = [
        str(alerta.get('Endereco_Origem') or alerta.get('ENDERECO_ORIGEM') or '').strip().upper(),
        str(alerta.get('SKU') or '').strip(),
        str(alerta.get('Data_Validade') or alerta.get('DATA_VALIDADE') or '').strip(),
        str(alerta.get('Tipo_Movimento') or alerta.get('TIPO_MOVIMENTO') or '').strip().upper(),
        str(alerta.get('Data_Transacao') or alerta.get('DATA_TRANSACAO') or '').strip(),
    ]
    return "K:" + "|".join(parts)

def _map_alerta_para_linha(alerta):
    def fmt_dt(v, with_time=False):
        try:
            if hasattr(v, 'strftime'):
                return v.strftime('%Y-%m-%d %H:%M' if with_time else '%Y-%m-%d')
        except Exception:
            pass
        return str(v) if v is not None else ''

    linha = {
        'ALERT_KEY': _alert_key_for_excel(alerta),
        'ID_MOVIMENTO': alerta.get('ID_Transacao') or alerta.get('ID_TRANSACAO') or 'N/A',
        'ENDERECO_RETIRADA': alerta.get('Endereco_Origem') or alerta.get('ENDERECO_ORIGEM'),
        'SKU': alerta.get('SKU'),
        'DESCRICAO_ITEM': alerta.get('Descricao_Item') or alerta.get('DESC_ITEM'),
        'DATA_VALIDADE': fmt_dt(alerta.get('Data_Validade') or alerta.get('DATA_VALIDADE')),
        'VOLUME_RETIRADO': alerta.get('Volume_Movimentado') or alerta.get('VOLUME_MOVIMENTADO'),
        'OCUPACAO_ORIGINAL': alerta.get('Ocupacao_Origem') or alerta.get('OCUPACAO_ORIGEM'),
        'DATA_TRANSACAO': fmt_dt(alerta.get('Data_Transacao') or alerta.get('DATA_TRANSACAO'), with_time=True),
        'MOTIVO': alerta.get('Motivo') or alerta.get('MOTIVO'),
        'OPORTUNIDADES_ENCONTRADAS': alerta.get('Oportunidades_Encontradas') or alerta.get('OPORTUNIDADES_ENCONTRADAS'),
        'TIPO_MOVIMENTO': alerta.get('Tipo_Movimento') or alerta.get('TIPO_MOVIMENTO'),
    }
    det = alerta.get('DETALHES_OPORTUNIDADES', []) or []
    for i in range(3):
        if i < len(det):
            op = det[i]
            linha[f'ENDERECO_ALTERNATIVO_{i+1}'] = op.get('ENDERECO_ALTERNATIVO','')
            linha[f'OCUPACAO_ALTERNATIVO_{i+1}'] = f"{op.get('OCUPACAO_ATUAL','')}/{op.get('CAPACIDADE','')}"
            linha[f'LIVRE_ALTERNATIVO_{i+1}'] = op.get('LIVRE','')
        else:
            linha[f'ENDERECO_ALTERNATIVO_{i+1}'] = ''
            linha[f'OCUPACAO_ALTERNATIVO_{i+1}'] = ''
            linha[f'LIVRE_ALTERNATIVO_{i+1}'] = ''
    return linha

def _excel_daily_path(base_dir: str) -> str:
    day = datetime.now().strftime("%Y%m%d")
    return os.path.join(base_dir, f"alertas_colmeia_{day}.xlsx")

def gerar_excel_alertas_acumulado(relatorio, base_dir):
    if not relatorio or relatorio.get('total_alertas', 0) == 0:
        log("Nenhum alerta para acumular no Excel")
        return None

    try:
        novos = pd.DataFrame([_map_alerta_para_linha(a) for a in relatorio['alertas']])
        if 'DATA_TRANSACAO' in novos.columns:
            try:
                novos['_DT_ORD'] = pd.to_datetime(novos['DATA_TRANSACAO'], errors='coerce', dayfirst=True)
                novos = novos.sort_values('_DT_ORD', ascending=False).drop(columns=['_DT_ORD'])
            except Exception:
                pass

        xlsx_path = _excel_daily_path(base_dir)

        if os.path.exists(xlsx_path):
            try:
                antigos = pd.read_excel(xlsx_path, sheet_name='Todos_Alertas')
            except Exception as e:
                log(f"[EXCEL] Falha ao ler existentes: {e} — vou recriar do zero.")
                antigos = pd.DataFrame(columns=list(novos.columns))

            if 'ALERT_KEY' not in antigos.columns:
                antigos['ALERT_KEY'] = ''

            combinado = pd.concat([antigos, novos], ignore_index=True)
            combinado = combinado.drop_duplicates(subset=['ALERT_KEY'], keep='first')

        else:
            combinado = novos

        resumo_sku = combinado.groupby(['SKU', 'DESCRICAO_ITEM'], dropna=False).agg(
            VOLUME_TOTAL=('VOLUME_RETIRADO', 'sum'),
            QTD_ALERTAS=('ALERT_KEY', 'count'),
            ENDERECOS_AFETADOS=('ENDERECO_RETIRADA', 'nunique')
        ).reset_index()

        resumo_end = combinado.groupby(['ENDERECO_RETIRADA'], dropna=False).agg(
            VOLUME_TOTAL=('VOLUME_RETIRADO', 'sum'),
            QTD_ALERTAS=('ALERT_KEY', 'count'),
            SKUS_DIFERENTES=('SKU', 'nunique')
        ).reset_index()

        with pd.ExcelWriter(xlsx_path, engine='openpyxl', mode='w') as writer:
            combinado.to_excel(writer, sheet_name='Todos_Alertas', index=False)
            resumo_sku.to_excel(writer, sheet_name='Resumo_SKU', index=False)
            resumo_end.to_excel(writer, sheet_name='Resumo_Endereco', index=False)

        log(f"[EXCEL] Atualizado (acumulado): {xlsx_path}  | Total no dia: {len(combinado)}")
        return xlsx_path

    except Exception as e:
        log(f"[EXCEL][ERRO] {e}")
        import traceback; log(traceback.format_exc())
        return None


def executar_auditoria_transacoes():
    log("=== EXECUTANDO AUDITORIA DE TRANSAÇÕES ===")
    try:
        base_dir = encontrar_pasta_onedrive_empresa()
        if not base_dir:
            log("Pasta do OneDrive não encontrada!")
            return

        caminho_estoque = build_estoque_posicao_if_needed(base_dir, max_age_minutes=30)
        if not caminho_estoque:
            log("[TRANS] Não foi possível gerar/encontrar estoque_posicao — abortando este ciclo.")
            return

        caminho_transacoes = encontrar_arquivo_transacoes(base_dir)
        if not caminho_transacoes:
            log("Arquivo de transações não encontrado!")
            return
        
        estoque_posicao = ler_estoque_posicao(caminho_estoque)
        transacoes_df = ler_transacoes(caminho_transacoes)
        
        if estoque_posicao is None or transacoes_df is None:
            log("Falha ao ler arquivos!")
            return
        
        transacoes_df = filtrar_enderecos_validos(transacoes_df)
        transacoes_recentes = filtrar_transacoes_recentes(transacoes_df, horas_retroativas=1)
        alertas = analisar_risco_colmeia(transacoes_recentes, estoque_posicao)
        
        if alertas:
            relatorio = {
                'total_alertas': len(alertas),
                'volume_total_movimentado': sum(alerta['VOLUME_MOVIMENTADO'] for alerta in alertas),
                'skus_afetados': len(set(alerta['SKU'] for alerta in alertas)),
                'alertas': alertas
            }
            
            excel_path = gerar_excel_alertas_acumulado(relatorio, base_dir)
            if excel_path:
                log(f"Relatório de alertas salvo: {excel_path}")
                
                enviar_email_alertas(relatorio, excel_path)
        
        log("Auditoria de transações concluída")
        
    except Exception as e:
        log(f"Erro na auditoria de transações: {e}")
        log(traceback.format_exc())

def enviar_email_alertas(relatorio, excel_path):
    try:
        assunto = f"[Auditoria Colmeia] {relatorio['total_alertas']} alertas de risco encontrados"
        
        corpo = f"""
        <h2>Alertas de Risco de Colmeia</h2>
        <p>Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
        
        <h3>Resumo:</h3>
        <ul>
            <li><strong>Total de alertas:</strong> {relatorio['total_alertas']}</li>
            <li><strong>Volume com risco:</strong> {relatorio['volume_total_movimentado']} unidades</li>
            <li><strong>SKUs afetados:</strong> {relatorio['skus_afetados']}</li>
        </ul>
        
        <p>Arquivo Excel com detalhes anexado.</p>
        <p>Este é um alerta automático do sistema de auditoria de colmeia.</p>
        """
        
        yag = yagmail.SMTP(user='mdiasbrancoautomacao@gmail.com', password='secwygmzlibyxhhh')
        yag.send(
            to=['douglas.lins2@mdiasbranco.com.br','leonardobarbosa.silva@mdiasbranco.com.br'
                'silvano.santos@mdiasbranco.com.br','edinaldo.lima@mdiasbranco.com.br',
                'jefferson.carlos@mdiasbranco.com.br','caroline.sobral@mdiasbranco.com.br',
                'jose.valentim@mdiasbranco.com.br','david.pereira@mdiasbranco.com.br',
                'inaldo.nascimento@mdiasbranco.com.br','gutemberg.queiroz@mdiasbranco.com.br',
                'alan.lira@mdiasbranco.com.br','jonas.leiras@mdiasbranco.com.br',
                'thaiane.pinheiro@mdiasbranco.com.br','sabrina.rodrigues@mdiasbranco.com.br',
                'genilda.miranda@mdiasbranco.com.br','aleson.souza@mdiasbranco.com.br'],
            subject=assunto,
            contents=corpo,
            attachments=excel_path
        )
        
        log("E-mail de alertas enviado com sucesso")
        
    except Exception as e:
        log(f"Falha ao enviar e-mail de alertas: {e}")

def _canon(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s.replace("-", " ").replace("_", " ")).strip().upper()
    return s

def _filtrar_tipo(df: pd.DataFrame, col_nome_candidatos=('TIPO_ENDERECO','TIPO ENDERECO','TIPO','TIPO_POSICAO'),
                  alvo=("DINAMICO","MEZANINO","PUSH BACK","PUSHBACK")) -> pd.DataFrame:
    try:
        col_tipo = _pick_col(df, list(col_nome_candidatos))
        if col_tipo != 'TIPO_ENDERECO':
            df = df.rename(columns={col_tipo: 'TIPO_ENDERECO'})
        df['_TIPO_CANON'] = df['TIPO_ENDERECO'].map(_canon)
        antes = len(df)
        df = df[df['_TIPO_CANON'].isin(set(alvo))].copy()
        df.drop(columns=['_TIPO_CANON'], inplace=True, errors='ignore')
        log(f"[TIPO] Filtro TIPO_ENDERECO aplicado: {antes} -> {len(df)}")
    except Exception as e:
        log(f"[TIPO][AVISO] Não foi possível aplicar filtro de tipo: {e}")
    return df

def _salvar_estoque_posicao(base_dir: str, estoque_posicao: pd.DataFrame) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(base_dir, f"estoque_posicao_{ts}.csv")
    df = estoque_posicao.copy()
    for col in ("DATA_VALIDADE","DATA_PRIMEIRO_PALLET"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
    try:
        df.to_csv(path, sep=";", index=False, encoding="utf-8-sig")
        log(f"[EXPORT] estoque_posicao salvo em: {path} ({len(df)} linhas)")
        limpar_estoques_posicao_antigos(base_dir, keep_last=1)

        return path
    except Exception as e:
        log(f"[EXPORT][ERRO] Falha ao salvar estoque_posicao: {e}")
        return ""


def _mtime_or_0(p):
    try:
        return os.path.getmtime(p)
    except Exception:
        return 0

def build_estoque_posicao_if_needed(base_dir: str, max_age_minutes: int = 30) -> str:
    atual = encontrar_arquivo_estoque_posicao(base_dir)
    atual_mtime = _mtime_or_0(atual) if atual else 0

    arquivos = os.listdir(base_dir)
    estoque_files = [f for f in arquivos if 'estoque_detalhado' in f.lower() and f.lower().endswith(('.csv','.xlsx','.xls'))]
    enderecos_files = [f for f in arquivos if 'cap_endereco' in f.lower() and f.lower().endswith(('.csv','.xlsx','.xls'))]

    if not estoque_files or not enderecos_files:
        log("[BOOT] Fontes (estoque_detalhado/cap_endereco) não encontradas — não há como gerar estoque_posicao.")
        return atual or ""

    estoque_path  = os.path.join(base_dir, estoque_files[0])
    enderecos_path= os.path.join(base_dir, enderecos_files[0])
    src_mtime = max(_mtime_or_0(estoque_path), _mtime_or_0(enderecos_path))

    need_build = False
    if not atual:
        need_build = True
        log("[BOOT] Não existe estoque_posicao — será criado agora.")
    else:
        age_min = (time.time() - atual_mtime) / 60.0
        if age_min > max_age_minutes and src_mtime > atual_mtime:
            need_build = True
            log(f"[BOOT] estoque_posicao está velho ({age_min:.1f} min) e fontes são mais novas — será recriado.")

    if not need_build:
        return atual

    try:
        est_df = ler_estoque(estoque_path)
        end_df = ler_enderecos(enderecos_path)

        est_df = _filtrar_tipo(est_df, alvo=("DINAMICO","PUSH BACK","PUSHBACK"))   
        end_df = _filtrar_tipo(end_df,  alvo=("DINAMICO","MEZANINO","PUSH BACK","PUSHBACK"))  

        miss = [c for c in ("BLOCO","COD_ENDERECO","CAPACIDADE") if c not in end_df.columns]
        if miss:
            log(f"[BOOT][ERRO] cap_endereco sem colunas: {miss}")
            return atual or ""

        miss2 = [c for c in ("BLOCO","COD_ENDERECO","COD_ITEM","DATA_VALIDADE","CHAVE_PALLET") if c not in est_df.columns]
        if miss2:
            log(f"[BOOT][ERRO] estoque_detalhado sem colunas: {miss2}")
            return atual or ""

        est_pos = criar_estoque_posicao(est_df, end_df)
        path = _salvar_estoque_posicao(base_dir, est_pos)
        return path or (atual or "")
    except Exception as e:
        log(f"[BOOT][ERRO] Falha ao construir estoque_posicao: {e}")
        log(traceback.format_exc())
        return atual or ""

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
    
    raise ValueError(f"Coluna não encontrada. Candidatos: {candidatos}")

def _read_csv_strict_build_df(path, encodings=("utf-8-sig","utf-8","latin1","cp1252")):
    EXPECTED_COLS = [
    "LOCAL_EXPEDICAO","COD_DEPOSITO","COD_ENDERECO","CHAVE_PALLET","VOLUME",
    "COD_ITEM","DESC_ITEM","LOTE","UOM","DOCUMENTO",
    "DATA_VALIDADE","DATA_ULTIMA_TRANSACAO","OCUPACAO",
    "CAPACIDADE","DESCRICAO","QTDE_POR_PALLET","PALLET_COMPLETO","BLOCO","TIPO_ENDERECO",
    "STATUS_PALLET","SHELF_ITEM","DATA_FABRICACAO","SHELF_ESTOQUE","DIAS_ESTOQUE","DIAS_VALIDADE","DATA_RELATORIO"
]

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

def ler_csv_corretamente(csv_path):
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

    return df

def ler_estoque(estoque_path):
    if estoque_path.lower().endswith('.csv'):
        df = ler_csv_corretamente(estoque_path)
    else:
        df = pd.read_excel(estoque_path)

    df.columns = [col.strip().upper() for col in df.columns]
    
    col_map = {
        'BLOCO': ['BLOCO', 'BLOCO_ENDERECO', 'LOCAL'],
        'COD_ENDERECO': ['COD_ENDERECO', 'ENDERECO', 'POSICAO'],
        'COD_ITEM': ['COD_ITEM', 'SKU', 'ITEM', 'PRODUTO'],
        'DATA_VALIDADE': ['DATA_VALIDADE', 'VALIDADE', 'DT_VALIDADE'],
        'CHAVE_PALLET': ['CHAVE_PALLET', 'PALLET', 'LOTE'],
        'DATA_ULTIMA_TRANSACAO': ['DATA_ULTIMA_TRANSACAO', 'DT_ULTIMA_TRANSACAO', 'ULTIMA_TRANSACAO']
    }
    
    for new_col, old_cols in col_map.items():
        try:
            old_col = _pick_col(df, old_cols)
            if old_col != new_col:
                df.rename(columns={old_col: new_col}, inplace=True)
        except ValueError:
            pass
    
    return df

def ler_enderecos(enderecos_path):
    if enderecos_path.lower().endswith('.csv'):
        df = ler_csv_corretamente(enderecos_path)
    else:
        df = pd.read_excel(enderecos_path)
    
    df.columns = [col.strip().upper() for col in df.columns]
    
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
        except ValueError:
            pass
    
    return df

def criar_estoque_posicao(estoque_df, enderecos_df):
    estoque_df['DATA_VALIDADE'] = pd.to_datetime(estoque_df['DATA_VALIDADE'], dayfirst=True, errors='coerce')
    
    grupo_cols = ['BLOCO', 'COD_ENDERECO', 'COD_ITEM', 'DATA_VALIDADE']
    estoque_agrupado = estoque_df.groupby(grupo_cols).agg(
        OCUP_PALLETS=('CHAVE_PALLET', 'nunique')
    ).reset_index()
    
    estoque_agrupado.rename(columns={'OCUP_PALLETS': 'OCUPACAO'}, inplace=True)
    
    estoque_posicao = pd.merge(
        estoque_agrupado,
        enderecos_df[['BLOCO', 'COD_ENDERECO', 'CAPACIDADE']],
        on=['BLOCO', 'COD_ENDERECO'],
        how='left'
    )
    
    estoque_posicao['LIVRE'] = estoque_posicao['CAPACIDADE'] - estoque_posicao['OCUPACAO']
    estoque_posicao['FLAG_CHEIO'] = (estoque_posicao['OCUPACAO'] >= estoque_posicao['CAPACIDADE']).astype(int)
    
    estoque_posicao['CHAVE_SKU_DATA'] = (
        estoque_posicao['COD_ITEM'].astype(str) + "|" + 
        pd.to_datetime(estoque_posicao['DATA_VALIDADE']).dt.strftime('%Y%m%d')
    )
    
    return estoque_posicao

def limpar_estoques_posicao_antigos(base_dir:str, keep_last: int=1):
    try:
        padroes=["estoque_posicao_*.csv","estoque_posicao_*.xlsx"]
        for padrao in padroes:
            arquivos = glob.glob(os.path.join(base_dir, padrao))
            arquivos.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            for f in arquivos[keep_last:]:
                try:
                    os.remove(f)
                    log(f"Removido antigo: {os.path.basename(f)}")
                except Exception as e:
                    log(f"[ERRO]{f}:{e}")
    except Exception as e:
        log(f"[CLEAN][ERRO] Falha na limpeza de estoque_posicao: {e}")

def calcular_indicadores(estoque_posicao, enderecos_df):
    log("Calculando indicadores (com logs)…")

    capacidade_por_endereco = estoque_posicao.groupby('COD_ENDERECO')['CAPACIDADE'].first()
    ocupacao_por_endereco = estoque_posicao.groupby('COD_ENDERECO')['OCUPACAO'].sum()
    
    total_cap = float(capacidade_por_endereco.sum())
    total_ocup = float(ocupacao_por_endereco.sum())
    cap_real=float(enderecos_df["CAPACIDADE"].sum())
    livre = total_cap - total_ocup
        
    porColmeia = (livre / cap_real) if cap_real else 0.0
    log(f"[COLMEIA] livre={livre:.0f}  cap_total={cap_real:.0f}  colmeia={porColmeia:.4f}")

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


def executar_auditoria_indicadores():
    """Executa auditoria de indicadores por turno"""
    log("=== EXECUTANDO AUDITORIA DE INDICADORES ===")
    
    try:
        fonte_dir = encontrar_pasta_onedrive_empresa()
        if not fonte_dir:
            log("Pasta do OneDrive não encontrada!")
            return
        
        arquivos = os.listdir(fonte_dir)
        estoque_files = [f for f in arquivos if 'estoque_detalhado' in f.lower() and f.lower().endswith(('.csv','.xlsx','.xls'))]
        enderecos_files = [f for f in arquivos if 'cap_endereco' in f.lower() and f.lower().endswith(('.csv','.xlsx','.xls'))]
        
        if not estoque_files or not enderecos_files:
            log("Arquivos necessários não encontrados!")
            return
        
        estoque_path = os.path.join(fonte_dir, estoque_files[0])
        enderecos_path = os.path.join(fonte_dir, enderecos_files[0])
        
        estoque_df = ler_estoque(estoque_path)
        enderecos_df = ler_enderecos(enderecos_path)
        
        if estoque_df is None or enderecos_df is None:
            log("Falha ao processar arquivos!")
            return
        
        estoque_posicao = criar_estoque_posicao(estoque_df, enderecos_df)
        indicadores = calcular_indicadores(estoque_posicao, enderecos_df)
        
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
        
        <p>Este é a relatório automático gerado pelo sistema de otimização de estoque.</p>
        """
        
        enviar_relatorio_email(assunto, corpo)
        log("Auditoria de indicadores concluída")
        
    except Exception as e:
        log(f"Erro na auditoria de indicadores: {e}")
        log(traceback.format_exc())

def enviar_relatorio_email(assunto, corpo):
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



def main():
    log("=" * 80)
    log("INICIANDO SISTEMA DE AUDITORIA DE ESTOQUE 24x7")
    log("=" * 80)
    log("Sistema iniciado. Pressione Ctrl+C para parar.")
    
    schedule.every(10).minutes.do(executar_auditoria_transacoes)
    
    schedule.every().day.at("13:00").do(executar_auditoria_indicadores)  # Turno A
    schedule.every().day.at("21:00").do(executar_auditoria_indicadores)  # Turno B  
    schedule.every().day.at("04:30").do(executar_auditoria_indicadores)  # Turno C
    
    executar_auditoria_transacoes()
    
    log("Sistema agendado. Auditoria de transações a cada 10 minutos.")
    log("Indicadores nos turnos: 13:00, 21:00, 04:30")
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  
        except KeyboardInterrupt:
            log("Sistema interrompido pelo usuário.")
            break
        except Exception as e:
            log(f"Erro no loop principal: {e}")
            time.sleep(300)  #

if __name__ == "__main__":
    main()