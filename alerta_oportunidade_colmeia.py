import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os

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

def ler_transacoes(caminho_transacoes):
    log_auditoria(f"Lendo transações de: {caminho_transacoes}")
    
    try:
        if caminho_transacoes.lower().endswith('.csv'):
            transacoes_df = pd.read_csv(caminho_transacoes, sep=';', encoding='latin1')
        else:
            transacoes_df = pd.read_excel(caminho_transacoes)
        
        transacoes_df.columns = [col.strip().upper() for col in transacoes_df.columns]
        log_auditoria(f"Transações lidas - Shape: {transacoes_df.shape}")
        log_auditoria(f"Colunas: {list(transacoes_df.columns)}")
        
        return transacoes_df
    except Exception as e:
        log_auditoria(f"Erro ao ler transações: {e}")
        return None

def filtrar_transacoes_recentes(transacoes_df, dias_retroativos=7):
    log_auditoria(f"Filtrando transações dos últimos {dias_retroativos} dias")
    
    try:
        transacoes_df['CREATED_AT'] = pd.to_datetime(
            transacoes_df['CREATED_AT'], 
            dayfirst=True, 
            errors='coerce'
        )
        
        data_limite = datetime.now() - pd.Timedelta(days=dias_retroativos)
        transacoes_recentes = transacoes_df[transacoes_df['CREATED_AT'] >= data_limite]
        
        log_auditoria(f"Transações recentes: {len(transacoes_recentes)}")
        return transacoes_recentes
    except Exception as e:
        log_auditoria(f"Erro ao filtrar transações: {e}")
        return transacoes_df

def analisar_risco_colmeia(transacoes_df, estoque_posicao):
    """Analisa risco de colmeia nas movimentações"""
    log_auditoria("=== INICIANDO ANÁLISE DE RISCO DE COLMEIA ===")
    
    alertas = []
    
    saidas = transacoes_df[transacoes_df['TIPO_MOVIMENTO'].str.upper() == 'SAIDA']
    log_auditoria(f"Total de saídas encontradas: {len(saidas)}")
    
    for idx, transacao in saidas.iterrows():
        try:
            sku = transacao['COD_ITEM']
            data_validade = pd.to_datetime(transacao['DATA_VALIDADE'], dayfirst=True, errors='coerce')
            volume = transacao['VOLUME']
            endereco_origem = transacao['COD_ENDERECO']
            
            if pd.isna(data_validade):
                continue
            
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
                    'ID_TRANSACAO': transacao['ID'],
                    'DATA_TRANSACAO': transacao['CREATED_AT'],
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
                log_auditoria(f"Alerta encontrado para transação {transacao['ID']} - SKU: {sku}")
                
        except Exception as e:
            log_auditoria(f"Erro ao processar transação {transacao.get('ID', 'N/A')}: {e}")
            continue
    
    log_auditoria(f"Total de alertas de colmeia encontrados: {len(alertas)}")
    return alertas

def gerar_relatorio_alertas(alertas):
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
            'Data_Transacao': alerta['DATA_TRANSACAO'].strftime('%d/%m/%Y %H:%M') if pd.notna(alerta['DATA_TRANSACAO']) else 'N/A',
            'SKU': alerta['SKU'],
            'Descricao_Item': alerta['DESC_ITEM'],
            'Volume_Movimentado': alerta['VOLUME_MOVIMENTADO'],
            'Endereco_Origem': alerta['ENDERECO_ORIGEM'],
            'Oportunidades_Encontradas': alerta['OPORTUNIDADES_ENCONTRADAS'],
            'Motivo': alerta['MOTIVO']
        })
    
    return relatorio

def executar_auditoria_colmeia(base_dir, estoque_posicao):
    """Função principal da auditoria de colmeia"""
    log_auditoria("=== EXECUTANDO AUDITORIA DE RISCO DE COLMEIA ===")
    
    arquivos = os.listdir(base_dir)
    transacoes_files = [f for f in arquivos if 'historico_transacoes' in f.lower() and f.lower().endswith(('.csv','.xlsx','.xls'))]
    
    if not transacoes_files:
        log_auditoria("Arquivo de transações não encontrado!")
        return None
    
    caminho_transacoes = os.path.join(base_dir, transacoes_files[0])
    
    # Ler transações
    transacoes_df = ler_transacoes(caminho_transacoes)
    if transacoes_df is None:
        return None
    
    # Filtrar transações recentes
    transacoes_recentes = filtrar_transacoes_recentes(transacoes_df, dias_retroativos=3)
    
    # Analisar risco de colmeia
    alertas = analisar_risco_colmeia(transacoes_recentes, estoque_posicao)
    
    # Gerar relatório
    relatorio = gerar_relatorio_alertas(alertas)
    
    if relatorio:
        log_auditoria(f"RELATÓRIO FINAL - Alertas: {relatorio['total_alertas']}")
        log_auditoria(f"Volume total movimentado com risco: {relatorio['volume_total_movimentado']}")
        log_auditoria(f"SKUs afetados: {relatorio['skus_afetados']}")
        
        # Log detalhado dos primeiros alertas
        for i, alerta in enumerate(relatorio['alertas_detalhados'][:5]):
            log_auditoria(f"Alerta {i+1}: {alerta}")
    
    return relatorio

# Função para integrar com o código principal
def adicionar_auditoria_colmeia_ao_main(main_function):
    """Decorator para adicionar auditoria de colmeia ao main existente"""
    def wrapper():
        # Executar o main original
        resultado_main = main_function()
        
        # Executar auditoria de colmeia se o main foi bem-sucedido
        if resultado_main and 'estoque_posicao' in resultado_main:
            log_auditoria("Iniciando auditoria de colmeia após processamento principal")
            relatorio_colmeia = executar_auditoria_colmeia(
                resultado_main['base_dir'], 
                resultado_main['estoque_posicao']
            )
            
            if relatorio_colmeia:
                # Adicionar ao resultado final
                resultado_main['auditoria_colmeia'] = relatorio_colmeia
        
        return resultado_main
    
    return wrapper

# Exemplo de uso integrado (modificar seu main existente):
"""
@adicionar_auditoria_colmeia_ao_main
def main():
    # Seu código main atual que retorna base_dir e estoque_posicao
    # ...
    return {
        'base_dir': BASE_DIR_AUD,
        'estoque_posicao': estoque_posicao,
        'indicadores': indicadores
    }
"""

# Para uso direto (teste independente):
if __name__ == "__main__":
    # Teste independente da auditoria
    BASE_DIR_TESTE = _find_onedrive_subfolder("Gestão de Estoque - Gestão_Auditoria")
    if BASE_DIR_TESTE:
        # Primeiro precisa criar o estoque_posicao (usando o código principal)
        # Esto é apenas para teste - na prática será integrado ao main
        log_auditoria("Executando teste independente da auditoria")
        
        # Aqui você precisaria ter o estoque_posicao carregado
        # Para teste, pode carregar de um arquivo temporário ou usar dados de exemplo
        pass