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
        if 'CHAVE_PALLET' in col.upper():
            coluna_pallet = col
            break
    
    if not coluna_pallet:
        raise ValueError("Coluna 'CHAVE_PALLET' não encontrada no arquivo de histórico")
    
    df_auditoria = pd.DataFrame(columns=['chave_pallete', 'status'])
    
    codigos_rastreabilidade = [str(codigo).strip() 
                             for codigo in df_rastreabilidade[coluna_rastreio].unique() 
                             if pd.notna(codigo) and str(codigo).strip()]
    
    log(f"\nIniciando análise de {len(codigos_rastreabilidade)} códigos de rastreabilidade...")
    
    for codigo in codigos_rastreabilidade:
        log(f"\n[DEBUG] Analisando código: {codigo}")
        
        movimentos = df_historico[df_historico[coluna_pallet].astype(str).str.strip() == codigo]
        
        log(f"[DEBUG] Movimentos encontrados para este código: {len(movimentos)}")
        
        if movimentos.empty:
            log("[DEBUG] Nenhum movimento encontrado para este código")
            df_auditoria = pd.concat([df_auditoria, pd.DataFrame([{
                'chave_pallete': codigo,
                'status': 'NÃO ENCONTRADO MOVIMENTAÇÃO'
            }])], ignore_index=True)
        else:
            log("[DEBUG] Exemplo de movimentos encontrados:")
            for i, (_, row) in enumerate(movimentos.head(3).iterrows()):
                log(f"{i+1}. MOTIVO: {row.get('MOTIVO', '')} | TIPO_MOVIMENTO: {row.get('TIPO_MOVIMENTO', '')}")
            
            tem_remessa_saida = False
            for _, row in movimentos.iterrows():
                motivo = str(row.get('MOTIVO', '')).strip().upper()
                tipo_movimento = str(row.get('TIPO_MOVIMENTO', '')).strip().upper()
                
                if motivo == 'REMESSA' and tipo_movimento == 'SAIDA':
                    tem_remessa_saida = True
                    break
            
            if tem_remessa_saida:
                status = 'OK - REMESSA E SAÍDA ENCONTRADAS'
                log("[DEBUG] Movimento com REMESSA e SAÍDA encontrado")
            else:
                status = 'MOVIMENTAÇÃO ENCONTRADA MAS SEM REMESSA/SAÍDA'
                log("[DEBUG] Nenhum movimento com REMESSA e SAÍDA encontrado")
            
            df_auditoria = pd.concat([df_auditoria, pd.DataFrame([{
                'chave_pallete': codigo,
                'status': status
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