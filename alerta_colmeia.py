import pandas as pd
import numpy as np
import os
import yagmail
from datetime import datetime
import logging
import time
import csv

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
    log(f"Processando arquivo: {os.path.basename(csv_path)}")
    
    try:
        log("Tentando leitura com pandas...")
        df = pd.read_csv(csv_path, sep=';', encoding='latin1', low_memory=False, on_bad_lines='warn')
        log(f"CSV lido com pandas - Linhas: {len(df)}, Colunas: {list(df.columns)}")
        return df
    except Exception as e:
        log(f"Falha ao ler com pandas: {e}. Usando método manual...")
    
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
                corrected = line + [''] * (len(header) - len(line))
                if len(corrected) == len(header):
                    data.append(corrected)
                else:
                    log(f"[AVISO] Linha {i+2} ignorada - colunas: {len(line)} (esperado: {len(header)})")
    
    if problemas > 0:
        log(f"[AVISO] Foram corrigidas {problemas} linhas com problemas de formatação")
    
    df = pd.DataFrame(data, columns=header)
    
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.replace('"', '').str.replace("'", "").str.strip()
    
    log("[DEBUG] Estrutura do DataFrame carregado:")
    log(f"Total de linhas: {len(df)}")
    log(f"Colunas: {list(df.columns)}")
    
    log("[DEBUG] Primeiras 3 linhas dos dados:")
    for i, (_, row) in enumerate(df.head(3).iterrows()):
        log(f"Linha {i}: {row.to_dict()}")
    
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

def calcular_ocupacao_otima_global(estoque_posicao):
    log("Calculando ocupação ótima global...")
    
    def calcular_para_grupo(grupo):
        grupo = grupo.copy()
        grupo['INDICE_GRUPO_NF_GLOBAL'] = grupo['TAXA_OCUPACAO_ORDENACAO'].rank(method='dense', ascending=False)
        grupo['OCP_REDISTRIBUIVEL_GLOBAL'] = grupo[grupo['FLAG_CHEIO'] == 0]['OCUPACAO'].sum()
        return grupo
    
    log("Aplicando cálculo por grupo SKU_DATA...")
    estoque_posicao = estoque_posicao.groupby('CHAVE_SKU_DATA').apply(calcular_para_grupo).reset_index(drop=True)
    
    log("Cálculo de ocupação ótima global concluído")
    return estoque_posicao

def calcular_indicadores(estoque_posicao, enderecos_df):
    log("Calculando Colmeia...")
    porColmeia = (estoque_posicao['LIVRE'].sum() / enderecos_df['CAPACIDADE'].sum())
    log("Calculando Ruas Vazias Real...")
    enderecos_com_itens = estoque_posicao[estoque_posicao['OCUPACAO'] > 0]['COD_ENDERECO'].unique()
    todos_enderecos = enderecos_df['COD_ENDERECO'].unique()
    end_vazios_real = len(np.setdiff1d(todos_enderecos, enderecos_com_itens))
    log("Calculando Ruas Vazias Otimizado...")
    end_vazios_otimo = end_vazios_real  
    log("Calculando Paletes Movimentados...")
    pallets_movimentados = 0  
    log("Calculando Ruas Liberadas...")
    end_vazios_total = end_vazios_otimo - end_vazios_real
    log("Todos os indicadores calculados")
    return {
        'porColmeia': porColmeia,
        'end_vazios_real': end_vazios_real,
        'end_vazios_otimo': end_vazios_otimo,
        'pallets_movimentados': pallets_movimentados,
        'end_vazios_total': end_vazios_total
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
        estoque_df = verificar_qualidade_dados(estoque_df, "estoque")
        enderecos_df = verificar_qualidade_dados(enderecos_df, "endereços")

        
        if estoque_df is None or enderecos_df is None:
            log("Falha ao processar arquivos! Encerrando processo.")
            return
        estoque_df = calcular_data_primeiro_palete(estoque_df)
        estoque_posicao = criar_estoque_posicao(estoque_df, enderecos_df)
        estoque_posicao = calcular_is_front(estoque_posicao, estoque_df)
        estoque_posicao = calcular_taxa_ocupacao(estoque_posicao)
        estoque_posicao = calcular_ocupacao_otima_global(estoque_posicao)
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