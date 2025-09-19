import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import os
import glob

def setup_auditoria_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"auditoria_colmeia_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger_auditoria = setup_auditoria_logging()

def log_auditoria(mensagem):
    logger_auditoria.info(mensagem)

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
            
            log_auditoria(f"Total de linhas lidas: {len(lines)}")
            
            header_idx = None
            for i, ln in enumerate(lines):
                parts = ln.split(";")
                if ("ID" in parts) and ("COD_ITEM" in parts) and ("TIPO_MOVIMENTO" in parts):
                    header_idx = i
                    break
            
            if header_idx is None:
                header_idx = 0
                log_auditoria("Header não encontrado, usando primeira linha")

            header = _split_fix(lines[header_idx].split(";"), len(EXPECTED_COLS_TRANSACOES))
            
            rows = []
            for ln in lines[header_idx+1:]:
                parts = _split_fix(ln.split(";"), len(EXPECTED_COLS_TRANSACOES))
                rows.append(parts)

            df_out = pd.DataFrame(rows, columns=EXPECTED_COLS_TRANSACOES, dtype=str)
            df_out.columns = [c.strip() for c in df_out.columns]
            
            for c in df_out.columns:
                df_out[c] = df_out[c].astype(str).str.strip().str.replace('"', '').str.replace("'", "")
            
            log_auditoria(f"CSV lido com sucesso usando encoding: {enc}")
            return df_out
            
        except Exception as e:
            log_auditoria(f"Falha com encoding {enc}: {e}")
            last_exc = e
            continue
    
    raise last_exc if last_exc else RuntimeError("Falha ao ler CSV de transações com encodings testados.")

def encontrar_arquivo_mais_recente(base_dir, padrao):
    try:
        arquivos = glob.glob(os.path.join(base_dir, padrao))
        if arquivos:
            arquivo_mais_recente = max(arquivos, key=os.path.getmtime)
            log_auditoria(f"Arquivo encontrado: {os.path.basename(arquivo_mais_recente)}")
            return arquivo_mais_recente
        return None
    except Exception as e:
        log_auditoria(f"Erro ao buscar arquivo {padrao}: {e}")
        return None

def encontrar_arquivo_transacoes(base_dir):
    log_auditoria(f"Procurando arquivos de transações em: {base_dir}")
    
    padroes = ['*historico_transacoes*', '*transacoes*', '*movimentacoes*', '*historico*']
    extensoes = ['.csv', '.xlsx', '.xls']
    
    for padrao in padroes:
        for extensao in extensoes:
            arquivo = encontrar_arquivo_mais_recente(base_dir, f"{padrao}{extensao}")
            if arquivo:
                return arquivo
    
    log_auditoria("Nenhum arquivo de transações encontrado")
    return None

def encontrar_arquivo_estoque_posicao(base_dir):
    log_auditoria(f"Procurando arquivo estoque_posicao em: {base_dir}")
    
    padroes = ['estoque_posicao_*.csv', 'estoque_posicao_*.xlsx']
    
    for padrao in padroes:
        arquivo = encontrar_arquivo_mais_recente(base_dir, padrao)
        if arquivo:
            return arquivo
    
    log_auditoria("Nenhum arquivo estoque_posicao encontrado")
    return None

def ler_estoque_posicao(caminho_estoque):
    log_auditoria(f"Lendo estoque_posicao de: {caminho_estoque}")
    
    try:
        if caminho_estoque.lower().endswith('.csv'):
            df = pd.read_csv(caminho_estoque, sep=';', encoding='utf-8-sig', low_memory=False)
        else:
            df = pd.read_excel(caminho_estoque)
        
        log_auditoria(f"Estoque_posicao lido - Shape: {df.shape}")
        
        letras_validas = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                           'I13','I14', 'J', 'K', 'Q', 'R17', 'S', 'T', 'U', 'V']
        mascara_enderecos_validos = df['COD_ENDERECO'].str.upper().str[0].isin(letras_validas)
        df = df[mascara_enderecos_validos].copy()
        
        log_auditoria(f"Estoque_posicao após filtro de endereços - Shape: {df.shape}")
        log_auditoria(f"Colunas: {list(df.columns)}")
        
        colunas_data = ['DATA_VALIDADE', 'DATA_PRIMEIRO_PALLET']
        for col in colunas_data:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
    except Exception as e:
        log_auditoria(f"Erro ao ler estoque_posicao: {e}")
        return None

def ler_transacoes(caminho_transacoes):
    log_auditoria(f"Lendo transações com método robusto de: {caminho_transacoes}")
    
    try:
        if caminho_transacoes.lower().endswith('.csv'):
            transacoes_df = _read_csv_transacoes_strict(caminho_transacoes)
        else:
            transacoes_df = pd.read_excel(caminho_transacoes)
            transacoes_df.columns = [col.strip().upper() for col in transacoes_df.columns]
        
        log_auditoria(f"Transações lidas - Shape: {transacoes_df.shape}")
        log_auditoria(f"Colunas: {list(transacoes_df.columns)}")
        
        if 'VOLUME' in transacoes_df.columns:
            transacoes_df['VOLUME'] = (
                transacoes_df['VOLUME']
                .astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(r"[^0-9\.\-]", "", regex=True)
            )
            transacoes_df['VOLUME'] = pd.to_numeric(transacoes_df['VOLUME'], errors="coerce")
        
        colunas_data = ['DATA_VALIDADE', 'CREATED_AT']
        for col in colunas_data:
            if col in transacoes_df.columns:
                transacoes_df[col] = pd.to_datetime(transacoes_df[col], dayfirst=True, errors="coerce")
        
        return transacoes_df
    except Exception as e:
        log_auditoria(f"Erro ao ler transações: {e}")
        return None

def filtrar_transacoes_recentes(transacoes_df, horas_retroativas=1):
    log_auditoria(f"Filtrando transações das últimas {horas_retroativas} hora(s) do dia atual")
    
    try:
        if 'CREATED_AT' not in transacoes_df.columns:
            log_auditoria("Coluna CREATED_AT não encontrada - usando todas as transações")
            return transacoes_df

        datas_validas = transacoes_df['CREATED_AT'].notnull()
        if datas_validas.sum() == 0:
            log_auditoria("Nenhuma data válida encontrada em CREATED_AT - usando todas as transações")
            return transacoes_df
        
        if not pd.api.types.is_datetime64_any_dtype(transacoes_df['CREATED_AT']):
            transacoes_df['CREATED_AT'] = pd.to_datetime(transacoes_df['CREATED_AT'], errors='coerce')
        
        agora = datetime.now()
        data_limite = agora - timedelta(hours=horas_retroativas)
        
        data_hoje = agora.replace(hour=0, minute=0, second=0, microsecond=0)
        
        log_auditoria(f"Agora: {agora}")
        log_auditoria(f"Data limite: {data_limite}")
        log_auditoria(f"Data hoje (início do dia): {data_hoje}")
        
        mascara_recentes = (
            (transacoes_df['CREATED_AT'] >= data_limite) & 
            (transacoes_df['CREATED_AT'] <= agora) &
            (transacoes_df['CREATED_AT'] >= data_hoje)  
        )
        
        transacoes_recentes = transacoes_df[mascara_recentes].copy()
        
        log_auditoria(f"Transações das últimas {horas_retroativas} hora(s) do dia atual: {len(transacoes_recentes)}")
        
        if len(transacoes_recentes) > 0:
            data_min = transacoes_recentes['CREATED_AT'].min()
            data_max = transacoes_recentes['CREATED_AT'].max()
            log_auditoria(f"Range de datas recentes: {data_min} até {data_max}")
            
            transacoes_por_hora = transacoes_recentes['CREATED_AT'].dt.floor('H').value_counts().sort_index()
            log_auditoria(f"Transações por hora: {transacoes_por_hora.to_dict()}")
            
            if 'TIPO_MOVIMENTO' in transacoes_recentes.columns:
                movimentos = transacoes_recentes['TIPO_MOVIMENTO'].value_counts()
                log_auditoria(f"Tipos de movimento: {movimentos.to_dict()}")
            
        else:
            datas_disponiveis = transacoes_df['CREATED_AT'].dropna()
            if len(datas_disponiveis) > 0:
                data_min_total = datas_disponiveis.min()
                data_max_total = datas_disponiveis.max()
                log_auditoria(f"Range total de datas disponíveis: {data_min_total} até {data_max_total}")
                
                datas_hoje = datas_disponiveis[datas_disponiveis >= data_hoje]
                log_auditoria(f"Transações do dia atual: {len(datas_hoje)}")
                
                if len(datas_hoje) > 0:
                    ultimas_datas = datas_hoje.sort_values(ascending=False).head(10)
                    log_auditoria(f"Últimas 10 datas de hoje: {ultimas_datas.tolist()}")
            
        return transacoes_recentes
            
    except Exception as e:
        log_auditoria(f"Erro ao filtrar transações: {e}")
        import traceback
        log_auditoria(f"Traceback: {traceback.format_exc()}")
        return transacoes_df

def filtrar_enderecos_validos(transacoes_df):
    log_auditoria("Filtrando endereços válidos (A-K, Q-V)")
    
    letras_validas = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                        'I13','I14', 'J', 'K', 'Q', 'R17', 'S', 'T', 'U', 'V']
    
    mascara_enderecos_validos = transacoes_df['COD_ENDERECO'].str.upper().str[0].isin(letras_validas)
    
    transacoes_filtradas = transacoes_df[mascara_enderecos_validos].copy()
    
    log_auditoria(f"Total de transações antes do filtro: {len(transacoes_df)}")
    log_auditoria(f"Total de transações após filtro de endereços: {len(transacoes_filtradas)}")
    log_auditoria(f"Transações removidas: {len(transacoes_df) - len(transacoes_filtradas)}")
    
    if len(transacoes_filtradas) > 0:
        enderecos_unicos = transacoes_filtradas['COD_ENDERECO'].unique()[:10]
        log_auditoria(f"Exemplos de endereços válidos: {enderecos_unicos}")
    
    return transacoes_filtradas

def analisar_risco_colmeia(transacoes_df, estoque_posicao):
    log_auditoria("=== INICIANDO ANÁLISE DE RISCO DE COLMEIA (VERSÃO OTIMIZADA) ===")
    
    alertas = []
    
    transacoes_df = filtrar_enderecos_validos(transacoes_df)
    
    saidas = transacoes_df[transacoes_df['TIPO_MOVIMENTO'].str.upper() == 'SAIDA']
    log_auditoria(f"Total de saídas encontradas: {len(saidas)}")
    
    if len(saidas) == 0:
        log_auditoria("Nenhuma saída encontrada para análise")
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
    
    for idx, transacao in saidas.iterrows():
        try:
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
            
            log_auditoria(f"Saída de endereço que estava CHEIO: {endereco_origem}, SKU: {sku}")
            
            chave_sku_data = f"{sku}|{data_validade.strftime('%Y%m%d')}"
            
            outros_enderecos = estoque_posicao[
                (estoque_posicao['CHAVE_SKU_DATA'] == chave_sku_data) &
                (estoque_posicao['COD_ENDERECO'] != endereco_origem)
            ]
            
            if len(outros_enderecos) == 0:
                continue  
            
            enderecos_com_capacidade = outros_enderecos[
                outros_enderecos['LIVRE'] > 0
            ]
            
            if len(enderecos_com_capacidade) > 0:
                alerta = {
                    'ID_TRANSACAO': transacao.get('ID', 'N/A'),
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
                        'LIVRE': oportunidade['LIVRE'],
                        'TAXA_OCUPACAO': f"{oportunidade['OCUPACAO']/oportunidade['CAPACIDADE']*100:.1f}%"
                    })
                
                alertas.append(alerta)
                log_auditoria(f"Alerta de colmeia encontrado: {endereco_origem} -> SKU: {sku}")
                
        except Exception as e:
            log_auditoria(f"Erro ao processar transação {transacao.get('ID', 'N/A')}: {e}")
            continue
    
    log_auditoria(f"Total de alertas de colmeia CRÍTICOS encontrados: {len(alertas)}")
    
    if alertas:
        skus_alertas = set(alerta['SKU'] for alerta in alertas)
        volume_total = sum(alerta['VOLUME_MOVIMENTADO'] for alerta in alertas)
        log_auditoria(f"SKUs com alertas críticos: {len(skus_alertas)}")
        log_auditoria(f"Volume total movimentado criticamente: {volume_total}")
    
    return alertas

def gerar_relatorio_alertas(alertas):
    """Gera relatório detalhado dos alertas"""
    if not alertas:
        log_auditoria("Nenhum alerta de colmeia encontrado")
        return None
    
    relatorio = {
        'total_alertas': len(alertas),
        'volume_total_movimentado': sum(alerta['VOLUME_MOVIMENTADO'] for alerta in alertas),
        'skus_afetados': len(set(alerta['SKU'] for alerta in alertas)),
        'alertas_detalhados': []
    }
    
    for alerta in alertas:
        relatorio['alertas_detalhados'].append({
            'ID_Transacao': alerta['ID_TRANSACAO'],
            'Data_Transacao': alerta['DATA_TRANSACAO'].strftime('%d/%m/%Y %H:%M') if hasattr(alerta['DATA_TRANSACAO'], 'strftime') else str(alerta['DATA_TRANSACAO']),
            'SKU': alerta['SKU'],
            'Descricao_Item': alerta['DESC_ITEM'],
            'Volume_Movimentado': alerta['VOLUME_MOVIMENTADO'],
            'Endereco_Origem': alerta['ENDERECO_ORIGEM'],
            'Ocupacao_Origem': f"{alerta['OCUPACAO_ORIGEM']}/{alerta['CAPACIDADE_ORIGEM']}",
            'Data_Validade': alerta['DATA_VALIDADE'].strftime('%d/%m/%Y') if hasattr(alerta['DATA_VALIDADE'], 'strftime') else str(alerta['DATA_VALIDADE']),
            'Motivo': alerta['MOTIVO'],
            'Tipo_Movimento': alerta['TIPO_MOVIMENTO'],
            'Oportunidades_Encontradas': alerta['OPORTUNIDADES_ENCONTRADAS'],
            'DETALHES_OPORTUNIDADES': alerta['DETALHES_OPORTUNIDADES']
        })
    
    return relatorio
def exibir_relatorio_final(relatorio):
    if not relatorio:
        log_auditoria("=== RELATÓRIO FINAL ===")
        log_auditoria("Nenhum alerta crítico de colmeia encontrado")
        return
    
    log_auditoria("=" * 80)
    log_auditoria("=== RELATÓRIO FINAL DE ALERTAS DE COLMEIA ===")
    log_auditoria("=" * 80)
    
    log_auditoria(f"Total de alertas críticos: {relatorio['total_alertas']}")
    log_auditoria(f"Volume total movimentado com risco: {relatorio['volume_total_movimentado']}")
    log_auditoria(f"SKUs afetados: {relatorio['skus_afetados']}")
    
    log_auditoria("")
    log_auditoria("=== DETALHES DOS PRINCIPAIS ALERTAS ===")
    
    for i, alerta in enumerate(relatorio['alertas_detalhados'][:10]):
        log_auditoria(f"Alerta {i+1}:")
        log_auditoria(f"  ID: {alerta['ID_Transacao']}")
        log_auditoria(f"  Data: {alerta['Data_Transacao']}")
        log_auditoria(f"  SKU: {alerta['SKU']} - {alerta['Descricao_Item']}")
        log_auditoria(f"  Volume: {alerta['Volume_Movimentado']}")
        log_auditoria(f"  Endereço: {alerta['Endereco_Origem']} (Ocupação: {alerta['Ocupacao_Origem']})")
        log_auditoria(f"  Data Validade: {alerta.get('Data_Validade', 'N/A')}")
        log_auditoria(f"  Oportunidades: {alerta['Oportunidades_Encontradas']}")
        log_auditoria(f"  Motivo: {alerta['Motivo']}")
        
        if 'DETALHES_OPORTUNIDADES' in alerta and alerta['DETALHES_OPORTUNIDADES']:
            for j, oportunidade in enumerate(alerta['DETALHES_OPORTUNIDADES'][:2]):  
                log_auditoria(f"  Oportunidade {j+1}: {oportunidade['ENDERECO_ALTERNATIVO']} "
                             f"(Ocupação: {oportunidade['OCUPACAO_ATUAL']}/{oportunidade['CAPACIDADE']}, "
                             f"Livre: {oportunidade['LIVRE']})")
        
        log_auditoria("")  
    
    if relatorio['total_alertas'] > 10:
        log_auditoria(f"... e mais {relatorio['total_alertas'] - 10} alertas")
    
    log_auditoria("")
    log_auditoria("=== ESTATÍSTICAS GERAIS ===")
    
    skus_volume = {}
    for alerta in relatorio['alertas_detalhados']:
        sku = alerta['SKU']
        volume = alerta['Volume_Movimentado']
        if sku in skus_volume:
            skus_volume[sku] += volume
        else:
            skus_volume[sku] = volume
    
    if skus_volume:
        top_skus = sorted(skus_volume.items(), key=lambda x: x[1], reverse=True)[:5]
        log_auditoria("Top 5 SKUs por volume:")
        for sku, volume in top_skus:
            log_auditoria(f"  SKU {sku}: {volume} unidades")
    
    enderecos_count = {}
    for alerta in relatorio['alertas_detalhados']:
        endereco = alerta['Endereco_Origem']
        if endereco in enderecos_count:
            enderecos_count[endereco] += 1
        else:
            enderecos_count[endereco] = 1
    
    if enderecos_count:
        top_enderecos = sorted(enderecos_count.items(), key=lambda x: x[1], reverse=True)[:5]
        log_auditoria("Top 5 endereços problemáticos:")
        for endereco, count in top_enderecos:
            log_auditoria(f"  {endereco}: {count} alertas")
    
    log_auditoria("=" * 80)
def executar_auditoria_colmeia(base_dir, estoque_posicao=None):
    log_auditoria("=== EXECUTANDO AUDITORIA DE RISCO DE COLMEIA ===")
    
    if estoque_posicao is None:
        caminho_estoque = encontrar_arquivo_estoque_posicao(base_dir)
        if not caminho_estoque:
            log_auditoria("Arquivo estoque_posicao não encontrado!")
            return None
        estoque_posicao = ler_estoque_posicao(caminho_estoque)
        if estoque_posicao is None:
            return None
    
    caminho_transacoes = encontrar_arquivo_transacoes(base_dir)
    if not caminho_transacoes:
        log_auditoria("Arquivo de transações não encontrado!")
        return None
    
    transacoes_df = ler_transacoes(caminho_transacoes)
    if transacoes_df is None:
        return None
    
    transacoes_df = filtrar_enderecos_validos(transacoes_df)
    
    transacoes_recentes = filtrar_transacoes_recentes(transacoes_df, horas_retroativas=1)
    
    alertas = analisar_risco_colmeia(transacoes_recentes, estoque_posicao)
    relatorio = gerar_relatorio_alertas(alertas)
    
    if relatorio and relatorio['total_alertas'] > 0:
        excel_path = gerar_excel_alertas(relatorio, base_dir, transacoes_df)
        if excel_path:
            log_auditoria(f"Relatório Excel salvo em: {excel_path}")
    
    if relatorio:
        log_auditoria(f"RELATÓRIO FINAL - Alertas: {relatorio['total_alertas']}")
        exibir_relatorio_final(relatorio)
    
    return relatorio
def gerar_excel_alertas(relatorio, base_dir, transacoes_df):
    """Gera arquivo Excel com os alertas de colmeia"""
    if not relatorio or relatorio['total_alertas'] == 0:
        log_auditoria("Nenhum alerta para gerar Excel")
        return None
    
    try:
        dados_excel = []
        
        for alerta_detalhado in relatorio['alertas_detalhados']:
            transacao_original = transacoes_df[
                transacoes_df['ID'] == alerta_detalhado['ID_Transacao']
            ].iloc[0] if 'ID_Transacao' in alerta_detalhado and alerta_detalhado['ID_Transacao'] != 'N/A' else None
            
            linha = {
                'ID_MOVIMENTO': alerta_detalhado['ID_Transacao'],
                'ENDERECO_RETIRADA': alerta_detalhado['Endereco_Origem'],
                'SKU': alerta_detalhado['SKU'],
                'DESCRICAO_ITEM': alerta_detalhado['Descricao_Item'],
                'DATA_VALIDADE': alerta_detalhado.get('Data_Validade', 'N/A'),
                'VOLUME_RETIRADO': alerta_detalhado['Volume_Movimentado'],
                'OCUPACAO_ORIGINAL': alerta_detalhado['Ocupacao_Origem'],
                'DATA_TRANSACAO': alerta_detalhado['Data_Transacao'],
                'MOTIVO': alerta_detalhado['Motivo'],
                'OPORTUNIDADES_ENCONTRADAS': alerta_detalhado['Oportunidades_Encontradas'],
                'LOGIN_USUARIO': transacao_original['CRIADO_POR_LOGIN'] if transacao_original is not None and 'CRIADO_POR_LOGIN' in transacao_original else 'N/A',
                'TIPO_MOVIMENTO': alerta_detalhado.get('Tipo_Movimento', 'SAIDA')
            }
            
            for i in range(3):
                chave_endereco = f'ENDERECO_ALTERNATIVO_{i+1}'
                chave_ocupacao = f'OCUPACAO_ALTERNATIVO_{i+1}'
                chave_livre = f'LIVRE_ALTERNATIVO_{i+1}'
                
                if i < alerta_detalhado['Oportunidades_Encontradas']:
                    oportunidade = alerta_detalhado['DETALHES_OPORTUNIDADES'][i]
                    linha[chave_endereco] = oportunidade['ENDERECO_ALTERNATIVO']
                    linha[chave_ocupacao] = f"{oportunidade['OCUPACAO_ATUAL']}/{oportunidade['CAPACIDADE']}"
                    linha[chave_livre] = oportunidade['LIVRE']
                else:
                    linha[chave_endereco] = ''
                    linha[chave_ocupacao] = ''
                    linha[chave_livre] = ''
            
            dados_excel.append(linha)
        
        df_excel = pd.DataFrame(dados_excel)
        
        df_excel = df_excel.sort_values('VOLUME_RETIRADO', ascending=False)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_path = os.path.join(base_dir, f"alertas_colmeia_{timestamp}.xlsx")
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df_excel.to_excel(writer, sheet_name='Todos_Alertas', index=False)
            
            resumo_sku = df_excel.groupby(['SKU', 'DESCRICAO_ITEM']).agg({
                'VOLUME_RETIRADO': 'sum',
                'OPORTUNIDADES_ENCONTRADAS': 'count',
                'ENDERECO_RETIRADA': 'nunique'
            }).reset_index()
            resumo_sku.columns = ['SKU', 'DESCRICAO', 'VOLUME_TOTAL', 'QTD_ALERTAS', 'ENDERECOS_AFETADOS']
            resumo_sku.to_excel(writer, sheet_name='Resumo_SKU', index=False)
            
            resumo_endereco = df_excel.groupby('ENDERECO_RETIRADA').agg({
                'VOLUME_RETIRADO': 'sum',
                'OPORTUNIDADES_ENCONTRADAS': 'count',
                'SKU': 'nunique'
            }).reset_index()
            resumo_endereco.columns = ['ENDERECO', 'VOLUME_TOTAL', 'QTD_ALERTAS', 'SKUS_DIFERENTES']
            resumo_endereco.to_excel(writer, sheet_name='Resumo_Endereco', index=False)
        
        log_auditoria(f"Arquivo Excel gerado: {excel_path}")
        log_auditoria(f"Total de alertas no Excel: {len(df_excel)}")
        
        return excel_path
        
    except Exception as e:
        log_auditoria(f"Erro ao gerar Excel: {e}")
        import traceback
        log_auditoria(f"Traceback: {traceback.format_exc()}")
        return None

        
    except Exception as e:
        log_auditoria(f"Erro ao gerar Excel: {e}")
        import traceback
        log_auditoria(f"Traceback: {traceback.format_exc()}")
        return None
if __name__ == "__main__":
    def encontrar_pasta_onedrive():
        user_dir = os.environ.get("USERPROFILE", "")
        for nome in os.listdir(user_dir):
            if "DIAS BRANCO" in nome.upper():
                raiz = os.path.join(user_dir, nome)
                subfolder = "Gestão de Estoque - Gestão_Auditoria"
                if os.path.isdir(raiz) and subfolder in os.listdir(raiz):
                    return os.path.join(raiz, subfolder)
        return None
    
    base_dir = encontrar_pasta_onedrive()
    if base_dir:
        log_auditoria(f"Executando auditoria no diretório: {base_dir}")
        relatorio = executar_auditoria_colmeia(base_dir)
    else:
        log_auditoria("Pasta do OneDrive não encontrada")