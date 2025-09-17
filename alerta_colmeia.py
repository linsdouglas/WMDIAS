import pandas as pd
import numpy as np
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def encontrar_pasta_onedrive_empresa():
    user_dir = os.environ["USERPROFILE"]
    possiveis = os.listdir(user_dir)
    for nome in possiveis:
        if "DIAS BRANCO" in nome.upper():
            caminho_completo = os.path.join(user_dir, nome)
            onedrive_dir = os.path.join(caminho_completo, "Gestão de Estoque - Gestão_Auditoria")
            if os.path.isdir(onedrive_dir):
                return onedrive_dir
    return None

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

def ler_csv_corretamente(csv_path):
    logger.info(f"Processando arquivo: {os.path.basename(csv_path)}")
    
    try:
        df = pd.read_csv(csv_path, sep=';', encoding='latin1', low_memory=False)
        logger.info(f"CSV lido com pandas - Linhas: {len(df)}, Colunas: {list(df.columns)}")
        return df
        
    except Exception as e:
        logger.warning(f"Falha ao ler com pandas: {e}. Tentando método manual...")
        
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
                    logger.warning(f"Linha {i+2} ignorada - colunas: {len(line)} (esperado: {len(header)})")
        
        if problemas > 0:
            logger.warning(f"Foram corrigidas {problemas} linhas com problemas")
        
        df = pd.DataFrame(data, columns=header)
        logger.info(f"CSV lido manualmente - Linhas: {len(df)}, Colunas: {list(df.columns)}")
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
        'DATA_VALIDADE_PURA': ['DATA_VALIDADE_PURA', 'VALIDADE', 'DT_VALIDADE'],
        'CHAVE_PALLET': ['CHAVE_PALLET', 'PALLET', 'LOTE'],
        'DATA_PRIMEIRO_PALLET': ['DATA_PRIMEIRO_PALLET', 'DT_PRIMEIRO_PALLET']
    }
    
    for new_col, old_cols in col_map.items():
        try:
            old_col = _pick_col(df, old_cols)
            if old_col != new_col:
                df.rename(columns={old_col: new_col}, inplace=True)
        except ValueError:
            logger.error(f"Coluna {new_col} não encontrada no estoque")
            return None
    
    return df

def ler_enderecos(enderecos_path):
    """Lê e processa arquivo de endereços"""
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
            logger.error(f"Coluna {new_col} não encontrada nos endereços")
            return None
    
    return df

def criar_estoque_posicao(estoque_df, enderecos_df):
    logger.info("Criando tabela Estoque_Posicao...")
    
    grupo_cols = ['BLOCO', 'COD_ENDERECO', 'COD_ITEM', 'DATA_VALIDADE_PURA']
    
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
        pd.to_datetime(estoque_posicao['DATA_VALIDADE_PURA']).dt.strftime('%Y%m%d')
    )
    
    estoque_posicao['CHAVE_POS'] = (
        estoque_posicao['BLOCO'].astype(str) + "|" +
        estoque_posicao['COD_ENDERECO'].astype(str) + "|" +
        estoque_posicao['COD_ITEM'].astype(str) + "|" +
        pd.to_datetime(estoque_posicao['DATA_VALIDADE_PURA']).dt.strftime('%Y%m%d')
    )
    
    return estoque_posicao

def calcular_is_front(estoque_posicao, estoque_df):
    logger.info("Calculando IsFront_RuaSKU...")
    
    primeiro_palete = estoque_df.groupby(['COD_ENDERECO', 'COD_ITEM']).agg(
        DATA_PRIMEIRO_PALLET=('DATA_PRIMEIRO_PALLET', 'min')
    ).reset_index()
    
    estoque_posicao = pd.merge(
        estoque_posicao,
        primeiro_palete,
        on=['COD_ENDERECO', 'COD_ITEM'],
        how='left'
    )
    
    estoque_posicao['ISFRONT_RUASKU'] = (
        (estoque_posicao['DATA_PRIMEIRO_PALLET'].notna()) &
        (pd.to_datetime(estoque_posicao['DATA_PRIMEIRO_PALLET']) == 
         pd.to_datetime(estoque_posicao['DATA_VALIDADE_PURA']))
    ).astype(int)
    
    return estoque_posicao

def calcular_taxa_ocupacao(estoque_posicao):
    estoque_posicao['TAXA_OCUPACAO_ORDENACAO'] = (
        estoque_posicao['OCUPACAO'] / estoque_posicao['CAPACIDADE'].replace(0, 1)
    ).fillna(0)
    return estoque_posicao

def calcular_ocupacao_otima_global(estoque_posicao):
    logger.info("Calculando ocupação ótima global...")
    
    def calcular_para_grupo(grupo):
        grupo = grupo.copy()
        grupo['INDICE_GRUPO_NF_GLOBAL'] = grupo['TAXA_OCUPACAO_ORDENACAO'].rank(method='dense', ascending=False)
        grupo['OCP_REDISTRIBUIVEL_GLOBAL'] = grupo[grupo['FLAG_CHEIO'] == 0]['OCUPACAO'].sum()
        return grupo
    
    estoque_posicao = estoque_posicao.groupby('CHAVE_SKU_DATA').apply(calcular_para_grupo).reset_index(drop=True)
    
    return estoque_posicao

def calcular_indicadores(estoque_posicao, enderecos_df):
    """Calcula todos os indicadores principais"""
    logger.info("Calculando indicadores...")
    
    porColmeia = (estoque_posicao['LIVRE'].sum() / enderecos_df['CAPACIDADE'].sum())
    
    enderecos_com_itens = estoque_posicao[estoque_posicao['OCUPACAO'] > 0]['COD_ENDERECO'].unique()
    todos_enderecos = enderecos_df['COD_ENDERECO'].unique()
    end_vazios_real = len(np.setdiff1d(todos_enderecos, enderecos_com_itens))
    
    end_vazios_otimo = end_vazios_real  
    
    pallets_movimentados = 0 
    
    end_vazios_total = end_vazios_otimo - end_vazios_real
    
    return {
        'porColmeia': porColmeia,
        'end_vazios_real': end_vazios_real,
        'end_vazios_otimo': end_vazios_otimo,
        'pallets_movimentados': pallets_movimentados,
        'end_vazios_total': end_vazios_total
    }

def enviar_email(indicadores, turno):
    """Envia email com os indicadores"""
    smtp_server = "smtp.office365.com"
    smtp_port = 587
    email_from = "seu_email@diasbranco.com"
    email_to = "seu_email@diasbranco.com"
    password = "sua_senha"
    
    subject = f"Relatório de Estoque - Turno {turno} - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    
    body = f"""
    <h2>Relatório de Otimização de Estoque</h2>
    <p>Turno: {turno}</p>
    <p>Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
    
    <h3>Indicadores:</h3>
    <ul>
        <li><strong>Colmeia (Espaço Livre):</strong> {indicadores['porColmeia']:.2%}</li>
        <li><strong>Ruas Vazias (Real):</strong> {indicadores['end_vazios_real']}</li>
        <li><strong>Ruas Vazias (Otimizado):</strong> {indicadores['end_vazios_otimo']}</li>
        <li><strong>Paletes Movimentados:</strong> {indicadores['pallets_movimentados']}</li>
        <li><strong>Ruas Liberadas:</strong> {indicadores['end_vazios_total']}</li>
    </ul>
    
    <p>Este é um relatório automático gerado pelo sistema de otimização.</p>
    """
    
    try:
        msg = MIMEMultipart()
        msg['From'] = email_from
        msg['To'] = email_to
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))
        
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(email_from, password)
        server.send_message(msg)
        server.quit()
        
        logger.info("Email enviado com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro ao enviar email: {e}")

def main():
    try:
        logger.info("Iniciando processo de análise de estoque...")
        
        fonte_dir = encontrar_pasta_onedrive_empresa()
        if not fonte_dir:
            logger.error("Pasta do OneDrive não encontrada!")
            return
        
        logger.info(f"Pasta encontrada: {fonte_dir}")
        
        arquivos = os.listdir(fonte_dir)
        
        estoque_files = [f for f in arquivos if 'estoque_detalhado' in f.lower() and f.lower().endswith(('.csv','.xlsx','.xls'))]
        enderecos_files = [f for f in arquivos if 'cap_endereco' in f.lower() and f.lower().endswith(('.csv','.xlsx','.xls'))]
        
        if not estoque_files or not enderecos_files:
            logger.error("Arquivos não encontrados!")
            return
        
        estoque_path = os.path.join(fonte_dir, estoque_files[0])
        enderecos_path = os.path.join(fonte_dir, enderecos_files[0])
        
        logger.info(f"Lendo estoque: {estoque_files[0]}")
        estoque_df = ler_estoque(estoque_path)
        
        logger.info(f"Lendo endereços: {enderecos_files[0]}")
        enderecos_df = ler_enderecos(enderecos_path)
        
        if estoque_df is None or enderecos_df is None:
            logger.error("Falha ao ler arquivos!")
            return
        
        estoque_posicao = criar_estoque_posicao(estoque_df, enderecos_df)
        estoque_posicao = calcular_is_front(estoque_posicao, estoque_df)
        estoque_posicao = calcular_taxa_ocupacao(estoque_posicao)
        estoque_posicao = calcular_ocupacao_otima_global(estoque_posicao)
        
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
        
        enviar_email(indicadores, turno)
        
        logger.info("Processo concluído com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro no processo principal: {e}")

if __name__ == "__main__":
    main()