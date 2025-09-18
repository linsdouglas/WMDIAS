import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import os
import glob

def setup_auditoria_logging():
    """Configura logging específico para a auditoria"""
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

def encontrar_arquivo_transacoes(base_dir):
    """Encontra o arquivo de transações mais recente"""
    log_auditoria(f"Procurando arquivos de transações em: {base_dir}")
    
    padroes = ['*historico_transacoes*', '*transacoes*', '*movimentacoes*']
    extensoes = ['.csv', '.xlsx', '.xls']
    
    for padrao in padroes:
        for extensao in extensoes:
            arquivos = glob.glob(os.path.join(base_dir, f"{padrao}{extensao}"))
            if arquivos:
                # Retorna o arquivo mais recente (por modificação)
                arquivo_mais_recente = max(arquivos, key=os.path.getmtime)
                log_auditoria(f"Arquivo de transações encontrado: {arquivo_mais_recente}")
                return arquivo_mais_recente
    
    log_auditoria("Nenhum arquivo de transações encontrado")
    return None

def ler_transacoes(caminho_transacoes):
    """Lê o arquivo de transações históricas"""
    log_auditoria(f"Lendo transações de: {caminho_transacoes}")
    
    try:
        if caminho_transacoes.lower().endswith('.csv'):
            transacoes_df = pd.read_csv(caminho_transacoes, sep=';', encoding='latin1', low_memory=False)
        else:
            transacoes_df = pd.read_excel(caminho_transacoes)
        
        transacoes_df.columns = [col.strip().upper() for col in transacoes_df.columns]
        log_auditoria(f"Transações lidas - Shape: {transacoes_df.shape}")
        log_auditoria(f"Colunas: {list(transacoes_df.columns)}")
        
        return transacoes_df
    except Exception as e:
        log_auditoria(f"Erro ao ler transações: {e}")
        return None

def filtrar_transacoes_recentes(transacoes_df, dias_retroativos=3):
    """Filtra transações dos últimos dias"""
    log_auditoria(f"Filtrando transações dos últimos {dias_retroativos} dias")
    
    try:
        # Converter coluna de data
        if 'CREATED_AT' in transacoes_df.columns:
            transacoes_df['CREATED_AT'] = pd.to_datetime(
                transacoes_df['CREATED_AT'], 
                dayfirst=True, 
                errors='coerce'
            )
            
            data_limite = datetime.now() - timedelta(days=dias_retroativos)
            transacoes_recentes = transacoes_df[transacoes_df['CREATED_AT'] >= data_limite]
            
            log_auditoria(f"Transações recentes: {len(transacoes_recentes)}")
            return transacoes_recentes
        else:
            log_auditoria("Coluna CREATED_AT não encontrada - usando todas as transações")
            return transacoes_df
            
    except Exception as e:
        log_auditoria(f"Erro ao filtrar transações: {e}")
        return transacoes_df

def analisar_risco_colmeia(transacoes_df, estoque_posicao):
    """Analisa risco de colmeia nas movimentações"""
    log_auditoria("=== INICIANDO ANÁLISE DE RISCO DE COLMEIA ===")
    
    alertas = []
    
    # Filtrar apenas saídas (movimentações que retiraram estoque)
    saidas = transacoes_df[transacoes_df['TIPO_MOVIMENTO'].str.upper() == 'SAIDA']
    log_auditoria(f"Total de saídas encontradas: {len(saidas)}")
    
    if len(saidas) == 0:
        log_auditoria("Nenhuma saída encontrada para análise")
        return alertas
    
    for idx, transacao in saidas.iterrows():
        try:
            sku = transacao['COD_ITEM']
            data_validade = pd.to_datetime(transacao['DATA_VALIDADE'], dayfirst=True, errors='coerce')
            volume = transacao['VOLUME']
            endereco_origem = transacao['COD_ENDERECO']
            
            if pd.isna(data_validade):
                log_auditoria(f"Data de validade inválida para transação {transacao.get('ID', 'N/A')}")
                continue
            
            # Formatar chave SKU_DATA para buscar no estoque
            chave_sku_data = f"{sku}|{data_validade.strftime('%Y%m%d')}"
            
            # Buscar esse SKU+Data em outros endereços
            outros_enderecos = estoque_posicao[
                (estoque_posicao['CHAVE_SKU_DATA'] == chave_sku_data) &
                (estoque_posicao['COD_ENDERECO'] != endereco_origem)
            ]
            
            if len(outros_enderecos) == 0:
                # Não há outros endereços com o mesmo item
                continue
            
            # Verificar se algum endereço alternativo tinha capacidade disponível
            enderecos_com_capacidade = outros_enderecos[
                outros_enderecos['LIVRE'] > 0
            ]
            
            if len(enderecos_com_capacidade) > 0:
                # RISCO DE COLMEIA: Havia endereços alternativos com capacidade
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
                    'OPORTUNIDADES_ENCONTRADAS': len(enderecos_com_capacidade),
                    'DETALHES_OPORTUNIDADES': []
                }
                
                # Adicionar detalhes das oportunidades
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
                log_auditoria(f"Alerta encontrado para transação {transacao.get('ID', 'N/A')} - SKU: {sku}")
                
        except Exception as e:
            log_auditoria(f"Erro ao processar transação {transacao.get('ID', 'N/A')}: {e}")
            continue
    
    log_auditoria(f"Total de alertas de colmeia encontrados: {len(alertas)}")
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
            'Oportunidades_Encontradas': alerta['OPORTUNIDADES_ENCONTRADAS'],
            'Motivo': alerta['MOTIVO']
        })
    
    return relatorio

def executar_auditoria_colmeia(base_dir, estoque_posicao):
    log_auditoria("=== EXECUTANDO AUDITORIA DE RISCO DE COLMEIA ===")
    
    caminho_transacoes = encontrar_arquivo_transacoes(base_dir)
    
    if not caminho_transacoes:
        log_auditoria("Arquivo de transações não encontrado!")
        return None
    
    transacoes_df = ler_transacoes(caminho_transacoes)
    if transacoes_df is None:
        return None
    
    transacoes_recentes = filtrar_transacoes_recentes(transacoes_df, dias_retroativos=3)
    
    alertas = analisar_risco_colmeia(transacoes_recentes, estoque_posicao)
    
    relatorio = gerar_relatorio_alertas(alertas)
    
    if relatorio:
        log_auditoria(f"RELATÓRIO FINAL - Alertas: {relatorio['total_alertas']}")
        log_auditoria(f"Volume total movimentado com risco: {relatorio['volume_total_movimentado']}")
        log_auditoria(f"SKUs afetados: {relatorio['skus_afetados']}")
        
        for i, alerta in enumerate(relatorio['alertas_detalhados'][:3]):
            log_auditoria(f"Alerta {i+1}: {alerta}")
    
    return relatorio

if __name__ == "__main__":
    from pathlib import Path
    
    base_dir_teste = Path.cwd()
    log_auditoria("Executando teste independente da auditoria")
    
    try:
        arquivos_estoque = glob.glob("estoque_posicao_*.csv")
        if arquivos_estoque:
            estoque_posicao = pd.read_csv(max(arquivos_estoque, key=os.path.getmtime), sep=';')
            relatorio = executar_auditoria_colmeia(base_dir_teste, estoque_posicao)
        else:
            log_auditoria("Nenhum arquivo estoque_posicao encontrado para teste")
    except Exception as e:
        log_auditoria(f"Erro no teste independente: {e}")