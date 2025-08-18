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

URL = "https://prod12cwlsistemas.mdb.com.br/sgr/#!/home"
ITEM_FILIAL = "M431 - Divisao Vitarella - Logistico"
ITEM_DEPOSITO = "LA01"
TIMEOUT = 15

global_username = ""
global_password = ""
driver = None
processo_ativo = False
loop_count = 0
critico_acumulado = 0
proximo_em_seg = 120

def log(msg):
    s = f"[{dt.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n"
    try:
        log_text.insert("end", s)
        log_text.see("end")
        janela.update_idletasks()
    except:
        print(s, end="")

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
        safe_click(driver,(By.XPATH, "//div[@class='item ng-scope' and @alt='Estoque Detalhado']"),"Relatório Alternativo")
        time.sleep(1)
        safe_click(driver,(By.XPATH, "//a[@class='logo' and @ui-sref='home']"),"Botão Home")
        time.sleep(2)
        return abrir_menu_relatorio(driver, actions, nome_relatorio)
    except Exception as e:
        log(f"[FALHA] Workaround final: {e}")
        return False

def abrir_menu_relatorio(driver, actions, nome_relatorio):
    try:
        menu_logistica = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//div[@class='ui dropdown item' and contains(text(),'Logística/Faturamento')]")))
        actions.move_to_element(menu_logistica).perform()
        time.sleep(0.5)
        menu_logistica.click()
    except Exception:
        safe_click(driver,(By.XPATH, "//div[@class='ui dropdown item' and contains(text(),'Logística/Faturamento')]"),"Menu Logística")
        time.sleep(1)
    try:
        submenu = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//div[@class='item ng-scope' and @alt='Relatórios WMDiaS']")))
        actions.move_to_element(submenu).perform()
        time.sleep(0.5)
        submenu.click()
    except Exception:
        try:
            safe_click(driver,(By.XPATH, "//div[@class='item ng-scope' and @alt='Relatórios WMDiaS']"),"Submenu Relatórios WMDiaS")
            time.sleep(1)
        except Exception:
            return quebrar_estado_e_recomeçar(driver, actions, nome_relatorio)
    try:
        item_final = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f"//div[@class='item ng-scope' and @alt='{nome_relatorio}']")))
        actions.move_to_element(item_final).click().perform()
        return True
    except Exception:
        try:
            safe_click(driver,(By.XPATH, f"//div[@class='item ng-scope' and @alt='{nome_relatorio}']"),f"Item {nome_relatorio}")
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'ui selection dropdown')]//input[@class='search']")))
            return True
        except Exception:
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

def baixar_e_mover_relatorio(driver, botao_download_webelement, nome_relatorio: str, destino_dir: str, download_dir: str | None = None, incluir_timestamp: bool = True) -> str | None:
    download_dir = download_dir or _default_download_dir()
    if not os.path.isdir(download_dir):
        return None
    if not destino_dir or not os.path.isdir(destino_dir):
        return None
    antes = _snapshot_downloads(download_dir)
    try:
        driver.execute_script("arguments[0].click();", botao_download_webelement)
    except Exception:
        botao_download_webelement.click()
    novo_arquivo = _esperar_novo_arquivo(download_dir, antes, timeout=240, estabilizacao_seg=1.2)
    if not novo_arquivo:
        return None
    base = nome_relatorio.strip().lower()
    if "rast" in base:
        base = "rastreabilidade"
    elif "hist" in base or "transa" in base:
        base = "historico_transacoes"
    else:
        base = re.sub(r"\s+", "_", base)
    if incluir_timestamp:
        base = f"{base}_{_timestamp()}"
    try:
        caminho_final = _mover_renomear(novo_arquivo, destino_dir, base)
        return caminho_final
    except Exception:
        return None

def baixar_relatorios_mais_recentes(driver, destino_dir=None, timeout_status=120):
    if destino_dir is None:
        destino_dir = fonte_dir
    if not destino_dir or not os.path.isdir(destino_dir):
        return False, 0
    def abrir_menu_manutencao_relatorio():
        try:
            safe_click(driver, (By.XPATH, "//div[@class='ui dropdown item' and @alt='Consulta']"), "Menu Consulta")
            safe_click(driver, (By.XPATH, "//div[contains(@class, 'item') and @alt='Manutenção Relatório']"), "Item Manutenção Relatório")
        except Exception as e:
            log(f"[ERRO] Manutenção Relatório: {e}")
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
        try:
            safe_click(driver, (By.XPATH, "//div[@class='ui dropdown item' and contains(text(),'Logística/Faturamento')]"), "Menu Logística")
            safe_click(driver, (By.XPATH, "//div[@class='item ng-scope' and @alt='Relatórios WMDiaS']"), "Submenu Relatórios WMDiaS")
            safe_click(driver, (By.XPATH, f"//div[@class='item ng-scope' and @alt='{nome_relatorio}']"), f"Item {nome_relatorio}")
            dias_passado = 30 if nome_relatorio == "Histórico Transações" else 2
            if not selecionar_unidade_embarcadora(driver):
                return False
            preencher_datas_e_executar(driver, dias_passado=dias_passado)
            return True
        except Exception as e:
            log(f"[ERRO] Reexecutar {nome_relatorio}: {e}")
            return False
    relatorios_desejados = ["Rastreabilidade", "Histórico Transações"]
    criticos = 0
    apagar_antigos(destino_dir)
    for relatorio in relatorios_desejados:
        tentativa = 0
        while tentativa < 2:
            tentativa += 1
            abrir_menu_manutencao_relatorio()
            linha = None
            status = None
            inicio = time.time()
            while (time.time() - inicio) < timeout_status:
                driver.refresh()
                time.sleep(3)
                linha, status = encontrar_linha_relatorio(relatorio)
                if linha is None:
                    pass
                elif status == "Executado":
                    break
                elif status == "Crítico":
                    criticos += 1
                    executar_relatorio(driver, relatorio)
                    break
                time.sleep(10)
            if not linha:
                break
            if status != "Executado":
                if tentativa == 1:
                    continue
                else:
                    break
            try:
                botao_download = linha.find_element(By.XPATH, ".//a[contains(@class, 'blue') and contains(@href, '/download-file/')]")
            except Exception as e:
                break
            nome_final = "Rastreabilidade" if "Rastre" in relatorio else "Histórico Transações"
            caminho_salvo = baixar_e_mover_relatorio(driver, botao_download, nome_final, destino_dir, None, True)
            if caminho_salvo:
                pass
            else:
                if tentativa == 1:
                    executar_relatorio(driver, relatorio)
                    continue
            break
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

def login_WMDIAS():
    try:
        username_field = WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH,"//input[@type='text' and @name='LOGIN']")))
        username_field.clear()
        username_field.send_keys(global_username)
        password_field = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//input[@type='password' and @name='SENHA']")))
        password_field.clear()
        password_field.send_keys(global_password)
        password_field.send_keys(Keys.ENTER)
        time.sleep(5)
        return True
    except Exception:
        return True

def preparar_driver():
    global driver
    edge_options = EdgeOptions()
    edge_options.use_chromium = True
    edge_options.add_argument("--start-maximized")
    edge_options.add_argument("--disable-dev-shm-usage")
    edge_options.add_argument("--no-sandbox")
    edge_options.debugger_address = "127.0.0.1:9222"
    service = EdgeService(executable_path="C://Users//xql80316//Downloads//edgedriver_win64//msedgedriver.exe")
    driver = webdriver.Edge(service=service, options=edge_options)

def ciclo():
    global processo_ativo, loop_count, critico_acumulado, proximo_em_seg
    if not fonte_dir:
        log("Pasta OneDrive não encontrada.")
        processo_ativo = False
        return
    os.makedirs(fonte_dir, exist_ok=True)
    try:
        if not driver:
            preparar_driver()
        driver.get(URL)
        login_WMDIAS()
        interacoes_sgr(driver)
    except Exception as e:
        log(f"[ERRO] Setup inicial: {e}")
        processo_ativo = False
        return
    while processo_ativo:
        try:
            ok, criticos = baixar_relatorios_mais_recentes(driver, destino_dir=fonte_dir, timeout_status=120)
            critico_acumulado += criticos
            if critico_acumulado >= 3:
                log("[ALERTA] Muitos relatórios Crítico. Encerrando loops.")
                processo_ativo = False
                break
            df = analisar_rastreabilidade_incremental(fonte_dir)
            loop_count += 1
            try:
                loop_count_label.configure(text=f"Loopings realizados: {loop_count}")
            except:
                pass
            if loop_count % 3 == 0:
                try:
                    driver.refresh()
                    time.sleep(3)
                    login_WMDIAS()
                except:
                    pass
            proximo_em_seg = 600
            for i in range(proximo_em_seg):
                if not processo_ativo:
                    break
                try:
                    timer_label.configure(text=f"Próximo loop em: {proximo_em_seg - i} s")
                    progress_bar.set((i+1)/proximo_em_seg)
                except:
                    pass
                time.sleep(1)
        except Exception as e:
            log(f"[FALHA LOOP] {e}")
            processo_ativo = False
            break
    try:
        progress_bar.set(0)
        timer_label.configure(text="Próximo loop em: 0 s")
    except:
        pass

def iniciar_processo():
    global processo_ativo, global_username, global_password
    if processo_ativo:
        return
    global_username = username_entry.get().strip()
    global_password = password_entry.get().strip()
    if not global_username or not global_password:
        messagebox.showerror("Erro", "Informe usuário e senha.")
        return
    processo_ativo = True
    t = threading.Thread(target=ciclo, daemon=True)
    t.start()

def parar_processo():
    global processo_ativo
    processo_ativo = False

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
timer_label = ctk.CTkLabel(frame_status, text="Próximo loop em: 600 s", font=("Arial", 10))
timer_label.grid(row=3, column=0, padx=5, pady=5, sticky="w")
progress_bar = ctk.CTkProgressBar(frame_status, orientation="horizontal", width=400)
progress_bar.grid(row=2, column=0, padx=5, pady=5, sticky="w")
rodape = ctk.CTkLabel(frame_status, text="Desenvolvido por Douglas Lins", font=("Arial", 8))
rodape.grid(row=5, column=0, padx=5, pady=5)
log_text = ctk.CTkTextbox(frame_status, width=400, height=200)
log_text.grid(row=1, column=0, padx=5, pady=5)
janela.mainloop()
