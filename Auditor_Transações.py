import os
import re
import time
import threading
import shutil
import datetime
import pandas as pd
from PIL import Image
from datetime import datetime as dt
import customtkinter as ctk
from tkinter import messagebox
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from queue import Queue, Empty
import builtins

URL = "https://prod12cwlsistemas.mdb.com.br/sgr/#!/home"
ITEM_FILIAL = "M431 - Divisao Vitarella - Logistico"
ITEM_DEPOSITO = "LA01"
TIMEOUT = 15

stop_event = threading.Event()
bg_thread = None
loop_count = 0
critico_acumulado = 0
driver = None
global_username = ""
global_password = ""


_log_queue = Queue()
_original_print = builtins.print

def log(msg):
    s = f"[{dt.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    try:
        _log_queue.put_nowait(s)
    except:
        pass
    try:
        _original_print(s)
    except:
        pass

def _print_ui(*args, **kwargs):
    try:
        s = " ".join(str(a) for a in args)
    except:
        s = ""
    try:
        _log_queue.put_nowait(s)
    except:
        pass
    try:
        _original_print(*args, **kwargs)
    except:
        pass

builtins.print = _print_ui

def _pump_logs():
    try:
        while True:
            line = _log_queue.get_nowait()
            try:
                log_text.insert("end", line + "\n")
                log_text.see("end")
            except:
                pass
    except Empty:
        pass
    try:
        janela.after(120, _pump_logs)
    except:
        pass

def _iniciar_log_ui():
    try:
        _pump_logs()
    except:
        pass

def get_resource_path(p):
    return p

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

def safe_click(driver, by_locator, nome_elemento="Elemento", timeout=10):
    try:
        element = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(by_locator))
        element.click()
    except (ElementClickInterceptedException, TimeoutException):
        try:
            element = WebDriverWait(driver, timeout).until(EC.presence_of_element_located(by_locator))
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", element)
        except Exception as js_e:
            log(f"[ERRO] Clique JS: {repr(js_e)}")
    except Exception as e:
        log(f"[ERRO] Clique: {repr(e)}")

def quebrar_estado_e_recomeçar(driver, actions, nome_relatorio):
    try:
        safe_click(driver,
            (By.XPATH, "//div[@class='item ng-scope' and @alt='Estoque Detalhado']"),
            nome_elemento="Relatório Alternativo")
        time.sleep(1)
        safe_click(driver,
            (By.XPATH, "//a[@class='logo' and @ui-sref='home']"),
            nome_elemento="Botão Home")
        time.sleep(2)
        return abrir_menu_relatorio(driver, actions, nome_relatorio)
    except Exception as fallback_e:
        log(f"[FALHA] Workaround final falhou: {fallback_e}")
        return False

def abrir_menu_relatorio(driver, actions, nome_relatorio):
    try:
        log(f"[SGR] Aguardando menu 'Logística/Faturamento'...")
        menu_logistica = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//div[@class='ui dropdown item' and contains(text(),'Logística/Faturamento')]"))
        )
        actions.move_to_element(menu_logistica).perform()
        time.sleep(0.5)
        menu_logistica.click()
        log("[SGR] Menu 'Logística/Faturamento' clicado.")
    except Exception:
        log("[ERRO] Falha ao clicar em 'Logística/Faturamento'. Tentando via JS...")
        safe_click(driver,
                   (By.XPATH, "//div[@class='ui dropdown item' and contains(text(),'Logística/Faturamento')]"),
                   nome_elemento="Menu Logística")
        time.sleep(1)
    try:
        log("[SGR] Aguardando submenu 'Relatórios WMDiaS'...")
        submenu = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//div[@class='item ng-scope' and @alt='Relatórios WMDiaS']"))
        )
        actions.move_to_element(submenu).perform()
        time.sleep(0.5)
        submenu.click()
        log("[SGR] Submenu 'Relatórios WMDiaS' clicado.")
    except Exception:
        log("[ERRO] Falha ao clicar em 'Relatórios WMDiaS'. Tentando via JS...")
        try:
            safe_click(driver,
                       (By.XPATH, "//div[@class='item ng-scope' and @alt='Relatórios WMDiaS']"),
                       nome_elemento="Submenu Relatórios WMDiaS")
            time.sleep(1)
        except Exception:
            log("[BUG] Submenu estava visível/clicável, mas deu erro. Aplicando workaround...")
            return quebrar_estado_e_recomeçar(driver, actions, nome_relatorio)
    try:
        log(f"[SGR] Aguardando item final '{nome_relatorio}'...")
        item_final = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//div[@class='item ng-scope' and @alt='{nome_relatorio}']"))
        )
        actions.move_to_element(item_final).click().perform()
        log(f"[SGR] Relatório '{nome_relatorio}' clicado com sucesso.")
        return True
    except Exception:
        log(f"[ERRO] Falha ao clicar em '{nome_relatorio}'. Tentando via JS...")

        try:
            safe_click(driver,
                (By.XPATH, f"//div[@class='item ng-scope' and @alt='{nome_relatorio}']"),
                nome_elemento=f"Item {nome_relatorio}")

            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'ui selection dropdown')]//input[@class='search']"))
            )

            log(f"[SGR] Relatório '{nome_relatorio}' clicado com sucesso via JS (com validação).")
            return True

        except Exception:
            log(f"[FALHA] Clique JS também falhou ou tela de filtro não apareceu para '{nome_relatorio}'. Aplicando fallback...")
            return quebrar_estado_e_recomeçar(driver, actions, nome_relatorio)

def selecionar_unidade_embarcadora(driver, item_embarcadora="M431"):
    for _ in range(3):
        try:
            dropdown = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'ui selection dropdown')]")))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", dropdown)
            dropdown.click()
            time.sleep(0.5)
            input_element = dropdown.find_element(By.XPATH, ".//input[@class='search']")
            input_element.clear()
            input_element.send_keys(item_embarcadora)
            time.sleep(0.5)
            input_element.send_keys(Keys.ENTER)
            return True
        except Exception:
            time.sleep(2)
    return False

def preencher_datas_e_executar(driver, dias_passado=2, dias_futuro=1):
    try:
        hoje = datetime.date.today()
        data_final = hoje + datetime.timedelta(days=dias_futuro)
        data_inicial = hoje - datetime.timedelta(days=dias_passado)
        driver.find_element(By.ID, "D1").clear()
        driver.find_element(By.ID, "D1").send_keys(data_inicial.strftime("%d/%m/%Y"))
        driver.find_element(By.ID, "D2").clear()
        driver.find_element(By.ID, "D2").send_keys(data_final.strftime("%d/%m/%Y"))
        driver.find_element(By.ID, "BTN_EXECUTAR").click()
        time.sleep(5)
    except Exception as e:
        log(f"[ERRO] Datas/Executar: {e}")

def interacoes_sgr(driver):
    driver.fullscreen_window()
    actions = ActionChains(driver)
    abrir_menu_relatorio(driver, actions, "Rastreabilidade")
    selecionar_unidade_embarcadora(driver)
    preencher_datas_e_executar(driver)

    abrir_menu_relatorio(driver, actions, "Histórico Transações")
    selecionar_unidade_embarcadora(driver)
    preencher_datas_e_executar(driver, dias_passado=30)

    abrir_menu_relatorio(driver, actions, "Estoque Detalhado")
    selecionar_unidade_embarcadora(driver)
    executar_relatorio_estoque(driver)

    return True

def _default_download_dir():
    return os.path.join(os.environ.get("USERPROFILE", ""), "Downloads")

def _is_temp_download(fname: str) -> bool:
    return fname.endswith(".crdownload") or fname.endswith(".tmp") or fname.endswith(".partial")

def _snapshot_downloads(download_dir: str) -> set:
    try:
        return {f for f in os.listdir(download_dir)}
    except Exception:
        return set()

def _esperar_novo_arquivo(download_dir: str, antes: set, timeout: int = 240, estabilizacao_seg: float = 1.2) -> str | None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        atuais = {f for f in os.listdir(download_dir)}
        novos = [f for f in list(atuais - antes) if not _is_temp_download(f)]
        if novos:
            candidatos = [os.path.join(download_dir, f) for f in novos if os.path.isfile(os.path.join(download_dir, f))]
            if candidatos:
                candidato = max(candidatos, key=lambda p: os.path.getmtime(p))
                try:
                    sz1 = os.path.getsize(candidato)
                    time.sleep(estabilizacao_seg)
                    sz2 = os.path.getsize(candidato)
                    if sz1 == sz2:
                        return candidato
                except Exception:
                    pass
        time.sleep(0.3)
    return None
def executar_relatorio_estoque(driver):
    try:
        executar_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "BTN_EXECUTAR"))
        )
        executar_btn.click()
        time.sleep(5)
        log("[SGR] Botão 'Executar' clicado para Estoque Detalhado.")
        return True
    except Exception as e:
        log(f"[ERRO] Não foi possível clicar no botão 'Executar' para Estoque Detalhado: {e}")
        return False


def baixar_e_mover_relatorio(driver,
                             botao_download_webelement,
                             nome_relatorio: str,
                             destino_dir: str,
                             download_dir: str | None = None) -> str | None:

    download_dir = download_dir or _default_download_dir()
    if not os.path.isdir(download_dir):
        log(f"[ERRO] Pasta de downloads inválida: {download_dir}")
        return None
    if not destino_dir or not os.path.isdir(destino_dir):
        log(f"[ERRO] Pasta de destino inválida: {destino_dir}")
        return None

    antes = _snapshot_downloads(download_dir)

    try:
        driver.execute_script("arguments[0].click();", botao_download_webelement)
    except Exception:
        botao_download_webelement.click()

    novo_arquivo = _esperar_novo_arquivo(download_dir, antes, timeout=240, estabilizacao_seg=1.2)
    if not novo_arquivo:
        log(f"[ERRO] Timeout esperando novo arquivo para '{nome_relatorio}'.")
        return None

    nome_fixo_map = {
        "Rastreabilidade": "rastreabilidade.xlsx",
        "Histórico Transações": "historico_transacoes.xlsx",
        "Estoque Detalhado": "estoque_detalhado.xlsx"
    }

    nome_final = nome_fixo_map.get(nome_relatorio, f"{nome_relatorio.lower().replace(' ', '_')}.xlsx")

    try:
        destino_path = os.path.join(destino_dir, nome_final)
        if os.path.exists(destino_path):
            os.remove(destino_path)

        caminho_final = os.path.join(destino_dir, nome_final)
        shutil.move(novo_arquivo, caminho_final)
        log(f"[MOVIDO] {os.path.basename(novo_arquivo)} → {caminho_final}")
        return caminho_final
    except Exception as e:
        log(f"[ERRO] Falha ao mover/renomear '{novo_arquivo}': {e}")
        return None


def _timestamp():
    return dt.now().strftime("%Y%m%d_%H%M")

def _unique_target_path(base_dir: str, base_name: str) -> str:
    name, ext = os.path.splitext(base_name)
    destino = os.path.join(base_dir, base_name)
    k = 2
    while os.path.exists(destino):
        destino = os.path.join(base_dir, f"{name}_({k}){ext}")
        k += 1
    return destino

def _mover_renomear(src_path: str, destino_dir: str, novo_stem: str) -> str:
    ext = os.path.splitext(src_path)[1]
    final_name = f"{novo_stem}{ext}"
    alvo = _unique_target_path(destino_dir, final_name)
    os.makedirs(destino_dir, exist_ok=True)
    shutil.move(src_path, alvo)
    return alvo

def apagar_antigos(destino_dir: str):
    try:
        for f in os.listdir(destino_dir):
            fl = f.lower()
            if fl.endswith(".csv") and ("rastreabilidade" in fl or "historico_transacoes" in fl):
                try:
                    os.remove(os.path.join(destino_dir, f))
                except:
                    pass
    except:
        pass

def baixar_relatorios_mais_recentes(driver, destino_dir=None, timeout_status=120):
    if destino_dir is None:
        destino_dir = fonte_dir
    if not destino_dir or not os.path.isdir(destino_dir):
        log(f"[ERRO] Pasta de destino inválida: {destino_dir}")
        return False, 0

    def abrir_menu_manutencao_relatorio():
        try:
            safe_click(driver, (By.XPATH, "//div[@class='ui dropdown item' and @alt='Consulta']"), "Menu Consulta")
            safe_click(driver, (By.XPATH, "//div[contains(@class, 'item') and @alt='Manutenção Relatório']"), "Item Manutenção Relatório")
        except Exception as e:
            log(f"[ERRO] Falha ao acessar Manutenção Relatório: {e}")

    def encontrar_linha_relatorio(titulo_desejado):
        linhas = driver.find_elements(By.XPATH, "//tbody/tr")
        for linha in linhas:
            try:
                titulo = linha.find_element(By.XPATH, ".//td[2]").text.strip()
                status = linha.find_element(By.XPATH, ".//td[4]").text.strip()
                if titulo_desejado in titulo:
                    return linha, status
            except:
                continue
        return None, None

    def executar_relatorio(driver, nome_relatorio):
        log(f"[REEXECUTAR] Reabrindo menu para relatório: {nome_relatorio}")
        safe_click(driver, (By.XPATH, "//div[@class='ui dropdown item' and contains(text(),'Logística/Faturamento')]"), "Menu Logística")
        safe_click(driver, (By.XPATH, "//div[@class='item ng-scope' and @alt='Relatórios WMDiaS']"), "Submenu Relatórios WMDiaS")
        safe_click(driver, (By.XPATH, f"//div[@class='item ng-scope' and @alt='{nome_relatorio}']"), f"Item {nome_relatorio}")

        if not selecionar_unidade_embarcadora(driver):
            return False

        if nome_relatorio != "Estoque Detalhado":
            dias_passado = 30 if nome_relatorio == "Histórico Transações" else 2
            preencher_datas_e_executar(driver, dias_passado=dias_passado)
        else:
            executar_relatorio_estoque(driver)
        return True

    relatorios_desejados = ["Rastreabilidade", "Histórico Transações", "Estoque Detalhado"]
    criticos = 0

    for relatorio in relatorios_desejados:
        tentativa = 0
        while tentativa < 2:  
            tentativa += 1
            abrir_menu_manutencao_relatorio()

            log(f"[INFO] Aguardando status 'Executado' para: {relatorio}")
            linha = None
            status = None
            inicio = time.time()

            while (time.time() - inicio) < timeout_status:
                driver.refresh()
                time.sleep(3)

                linha, status = encontrar_linha_relatorio(relatorio)
                if linha is None:
                    log(f"[ESPERA] Relatório '{relatorio}' ainda não apareceu.")
                elif status == "Executado":
                    log(f"[OK] Relatório '{relatorio}' está pronto para download.")
                    break
                elif status == "Crítico":
                    log(f"[ERRO] Relatório '{relatorio}' CRÍTICO. Reexecutando...")
                    executar_relatorio(driver, relatorio)
                    break
                else:
                    log(f"[AGUARDANDO] Status atual: '{status}' → '{relatorio}'. Nova tentativa em 10s...")
                time.sleep(10)

            if not linha:
                log(f"[ERRO] Relatório '{relatorio}' não encontrado após {timeout_status} segundos.")
                break

            if status != "Executado":
                if tentativa == 1:
                    log(f"[REAVISO] Tentando reexecutar o relatório '{relatorio}' por falha/timeout.")
                    continue
                else:
                    log(f"[ERRO] '{relatorio}' falhou novamente após reprocessamento.")
                    break

            try:
                botao_download = linha.find_element(By.XPATH, ".//a[contains(@class, 'blue') and contains(@href, '/download-file/')]")
            except Exception as e:
                log(f"[ERRO] Botão de download não encontrado para '{relatorio}': {e}")
                break

            if "Rastre" in relatorio:
                nome_final = "Rastreabilidade"
            elif "Hist" in relatorio:
                nome_final = "Histórico Transações"
            else:
                nome_final = "Estoque Detalhado"
                
            caminho_salvo = baixar_e_mover_relatorio(
                driver=driver,
                botao_download_webelement=botao_download,
                nome_relatorio=nome_final,
                destino_dir=destino_dir,
                download_dir=None,             
            )

            if caminho_salvo:
                log(f"[OK] '{relatorio}' salvo em: {caminho_salvo}")
                break
            else:
                log(f"[ERRO] Falha ao salvar '{relatorio}'.")
                if tentativa == 1:
                    executar_relatorio(driver, relatorio)
                    continue
                break

    try:
        safe_click(driver, (By.XPATH, "//a[@class='logo' and @ui-sref='home']"), "Botão Home")
        time.sleep(5)
    except Exception as e:
        log(f"[AVISO] Retorno à Home falhou: {e}")

    return True, criticos

def ler_csv_corretamente(csv_path):
    with open(csv_path, 'r', encoding='latin1') as f:
        lines = [line.strip().split(';') for line in f.readlines() if line.strip()]
    if not lines:
        raise ValueError("CSV vazio")
    header = lines[0]
    data = []
    for line in lines[1:]:
        if len(line) == len(header):
            data.append(line)
        else:
            corrected = line[:len(header)]
            if len(corrected) == len(header):
                data.append(corrected)
    df = pd.DataFrame(data, columns=header)
    return df

def filtrar_chaves_mes(df, coluna_chave):
    chaves = df[coluna_chave].astype(str).str.strip()
    mascara = chaves.str.startswith('M431') & chaves.str.endswith('M')
    return df[mascara].copy()

def analisar_rastreabilidade_incremental(fonte_dir):
    arquivos = os.listdir(fonte_dir)
    rastreabilidade_files = [f for f in arquivos if 'rastreabilidade' in f.lower() and f.endswith('.csv')]
    historico_files = [f for f in arquivos if 'historico_transacoes' in f.lower() and f.endswith('.csv')]
    if not rastreabilidade_files or not historico_files:
        return None
    rastreabilidade_path = os.path.join(fonte_dir, max(rastreabilidade_files, key=lambda x: os.path.getmtime(os.path.join(fonte_dir, x))))
    historico_path = os.path.join(fonte_dir, max(historico_files, key=lambda x: os.path.getmtime(os.path.join(fonte_dir, x))))
    auditoria_path = os.path.join(fonte_dir, "auditoria_24_7.xlsx")
    df_rastreabilidade = ler_csv_corretamente(rastreabilidade_path)
    df_historico = ler_csv_corretamente(historico_path)
    coluna_rastreio = None
    for col in df_rastreabilidade.columns:
        if 'COD_RASTREABILIDADE' in col.upper():
            coluna_rastreio = col
            break
    coluna_pallet = None
    for col in df_historico.columns:
        if 'CHAVE_PALLET' in col.upper():
            coluna_pallet = col
            break
    if not coluna_rastreio or not coluna_pallet:
        return None
    df_rastreabilidade = filtrar_chaves_mes(df_rastreabilidade, coluna_rastreio)
    df_historico = filtrar_chaves_mes(df_historico, coluna_pallet)
    if os.path.exists(auditoria_path):
        try:
            df_auditoria_existente = pd.read_excel(auditoria_path)
            ja_auditadas = set(df_auditoria_existente['chave_pallete'].astype(str).str.strip().tolist())
        except:
            df_auditoria_existente = pd.DataFrame(columns=['chave_pallete','status'])
            ja_auditadas = set()
    else:
        df_auditoria_existente = pd.DataFrame(columns=['chave_pallete','status'])
        ja_auditadas = set()
    codigos_rastreabilidade = [str(codigo).strip() for codigo in df_rastreabilidade[coluna_rastreio].unique() if pd.notna(codigo) and str(codigo).strip()]
    codigos_novos = [c for c in codigos_rastreabilidade if c not in ja_auditadas]
    if not codigos_novos:
        return df_auditoria_existente
    resultados = []
    for codigo in codigos_novos:
        movimentos = df_historico[df_historico[coluna_pallet].astype(str).str.strip() == codigo]
        if movimentos.empty:
            resultados.append({'chave_pallete': codigo, 'status': 'NÃO ENCONTRADO MOVIMENTAÇÃO'})
        else:
            tem_remessa_saida = False
            for _, row in movimentos.iterrows():
                motivo = str(row.get('MOTIVO', '')).strip().upper()
                tipo_movimento = str(row.get('TIPO_MOVIMENTO', '')).strip().upper()
                if motivo == 'REMESSA' and tipo_movimento == 'SAIDA':
                    tem_remessa_saida = True
                    break
            if tem_remessa_saida:
                resultados.append({'chave_pallete': codigo, 'status': 'OK - REMESSA E SAÍDA ENCONTRADAS'})
            else:
                resultados.append({'chave_pallete': codigo, 'status': 'MOVIMENTAÇÃO ENCONTRADA MAS SEM REMESSA/SAÍDA'})
    df_novos = pd.DataFrame(resultados)
    df_final = pd.concat([df_auditoria_existente, df_novos], ignore_index=True)
    df_final.to_excel(auditoria_path, index=False)
    return df_final

def login_sgr():
    try:
        username_field = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//input[@type='text' and @name='username']"))
        )
        log("Campo de usuário encontrado, efetuando login...")
        username_field.clear()
        username_field.send_keys(global_username)
        password_field = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//input[@type='password' and @name='password']"))
        )
        password_field.clear()
        password_field.send_keys(global_password)
        password_field.send_keys(Keys.ENTER)
        log("Login efetuado com sucesso.")
        time.sleep(5)
    except Exception as e:
        log("Login não requerido após o refresh ou elemento não encontrado")

def preparar_driver():
    global driver
    edge_options = EdgeOptions()
    edge_options.use_chromium = True
    edge_options.add_argument("--start-maximized")
    edge_options.add_argument("--disable-dev-shm-usage")
    edge_options.add_argument("--no-sandbox")
    service = EdgeService(executable_path="C://Users//xql80316//Downloads//edgedriver_win64//msedgedriver.exe")
    driver = webdriver.Edge(service=service, options=edge_options)

stop_event = threading.Event()
bg_thread = None
loop_interval = 120

def update_timer(remaining_time):
    if remaining_time >= 0:
        try:
            timer_label.configure(text=f"Próximo loop em: {remaining_time} s")
            progress_bar.set(1 - (remaining_time / loop_interval))
        except:
            pass
        janela.after(1000, update_timer, remaining_time - 1)
    else:
        try:
            timer_label.configure(text="Executando processo...")
        except:
            pass

def background_loop():
    global driver, loop_count, critico_acumulado
    try:
        if driver is None:
            log("Inicializando navegador...")
            preparar_driver()
        driver.get(URL)
        log("Abrindo URL e tentando login...")
        login_sgr()
        log("Solicitando relatórios...")
        interacoes_sgr(driver)
    except Exception as e:
        log(f"[ERRO] Setup inicial: {e}")
        return
    while not stop_event.is_set():
        try:
            ret = baixar_relatorios_mais_recentes(driver, destino_dir=fonte_dir, timeout_status=120)
            if isinstance(ret, tuple) and len(ret) == 2:
                ok, criticos = ret
            else:
                ok, criticos = bool(ret), 0
            critico_acumulado += criticos
            if critico_acumulado >= 3:
                log("[ALERTA] Muitos relatórios Crítico. Encerrando loops.")
                break
            _ = analisar_rastreabilidade_incremental(fonte_dir)
            loop_count += 1
            try:
                loop_count_label.configure(text=f"Loopings realizados: {loop_count}")
            except:
                pass
            if loop_count % 3 == 0:
                try:
                    log("Atualizando página e validando sessão...")
                    driver.refresh()
                    time.sleep(3)
                    login_sgr()
                except:
                    pass
            try:
                janela.after(0, update_timer, loop_interval)
            except:
                pass
            if stop_event.wait(loop_interval):
                break
        except Exception as e:
            log(f"[FALHA LOOP] {e}")
            break
    try:
        progress_bar.set(0)
        timer_label.configure(text="Próximo loop em: 0 s")
    except:
        pass

def iniciar_processo():
    global bg_thread, global_username, global_password
    global_username = username_entry.get().strip()
    global_password = password_entry.get().strip()
    if not global_username or not global_password:
        messagebox.showerror("Erro", "Informe o usuário e a senha para o login.")
        return
    if bg_thread is not None and bg_thread.is_alive():
        return
    stop_event.clear()
    bg_thread = threading.Thread(target=background_loop, daemon=True)
    bg_thread.start()
    try:
        messagebox.showinfo("Info", "Processo iniciado!")
    except:
        pass
    log("Processo iniciado!")

def parar_processo():
    global driver
    stop_event.set()
    try:
        messagebox.showinfo("Info", "Processo interrompido!")
    except:
        pass
    log("Processo interrompido!")
    try:
        if driver is not None:
            driver.quit()
            driver = None
    except:
        driver = None


ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")
janela = ctk.CTk()
janela.title("Auditor de Transações")
janela.geometry("450x550")
frame_login = ctk.CTkFrame(janela, fg_color ="transparent")
frame_login.pack(pady=10, padx=10, fill="x")
usuario_label = ctk.CTkLabel(frame_login, text="Usuário:", font=("Arial", 12))
usuario_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")
username_entry = ctk.CTkEntry(frame_login, width=200, placeholder_text="Digite seu usuário")
username_entry.grid(row=0, column=1, padx=5, pady=5)
def toggle_password():
    if password_entry.cget("show") == "*":
        password_entry.configure(show="")
        if eye_open_icon:
            olho_button.configure(image=eye_open_icon)
    else:
        password_entry.configure(show="*")
        if eye_closed_icon:
            olho_button.configure(image=eye_closed_icon)
eye_open_path = get_resource_path("C://Users//xql80316//OneDrive - M DIAS BRANCO S.A. INDUSTRIA E COMERCIO DE ALIMENTOS//Documentos//Automações EXCEL//PROJETO_CANHOTO//source//imagens//mostrar.png")
eye_closed_path = get_resource_path("C://Users//xql80316//OneDrive - M DIAS BRANCO S.A. INDUSTRIA E COMERCIO DE ALIMENTOS//Documentos//Automações EXCEL//PROJETO_CANHOTO//source//imagens//esconder.png")
if os.path.exists(eye_open_path) and os.path.exists(eye_closed_path):
    eye_open_icon = ctk.CTkImage(light_image=Image.open(eye_open_path), size=(20, 20))
    eye_closed_icon = ctk.CTkImage(light_image=Image.open(eye_closed_path), size=(20, 20))
else:
    eye_open_icon = None
    eye_closed_icon = None
senha_label = ctk.CTkLabel(frame_login, text="Senha:", font=("Arial", 12))
senha_label.grid(row=1, column=0, padx=5, pady=5, sticky="w")
password_entry = ctk.CTkEntry(frame_login, width=200, placeholder_text="Digite sua senha", show="*")
password_entry.grid(row=1, column=1, padx=5, pady=5)
olho_button = ctk.CTkButton(frame_login, text="", image=eye_closed_icon, width=30, height=25, command=toggle_password, fg_color="transparent")
olho_button.grid(row=1, column=2, padx=5, pady=5)
frame_controle = ctk.CTkFrame(janela, fg_color="transparent")
frame_controle.pack(pady=10, padx=10, fill="x")
botao_iniciar = ctk.CTkButton(frame_controle, text="Iniciar Processo", width=200, command=iniciar_processo)
botao_iniciar.pack(side="left", padx=5)
botao_parar = ctk.CTkButton(frame_controle, text="Parar Processo", width=200, command=parar_processo)
botao_parar.pack(side="left", padx=5)
frame_status = ctk.CTkFrame(janela, fg_color="transparent")
frame_status.pack(pady=10, padx=10, fill="both", expand=True)
loop_count_label = ctk.CTkLabel(frame_status, text="Loopings realizados: 0", font=("Arial", 12))
loop_count_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")
timer_label = ctk.CTkLabel(frame_status, text="Próximo loop em: 120 s", font=("Arial", 10))
timer_label.grid(row=3, column=0, padx=5, pady=5, sticky="w")
progress_bar = ctk.CTkProgressBar(frame_status, orientation="horizontal", width=400)
progress_bar.grid(row=2, column=0, padx=5, pady=5, sticky="w")
rodape = ctk.CTkLabel(frame_status, text="Desenvolvido por Douglas Lins", font=("Arial", 8))
rodape.grid(row=5, column=0, padx=5, pady=5)
log_text = ctk.CTkTextbox(frame_status, width=400, height=200)
log_text.grid(row=1, column=0, padx=5, pady=5)
_iniciar_log_ui()
janela.mainloop()
