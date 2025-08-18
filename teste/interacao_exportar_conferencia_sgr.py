import time
import os
import re
import datetime
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.action_chains import ActionChains

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

URL = "https://prod12cwlsistemas.mdb.com.br/sgr/#!/home"
USUARIO = "vit06329"
SENHA = "Ce112206@"
ITEM_FILIAL = "M431 - Divisao Vitarella - Logistico"
ITEM_DEPOSITO = "LA01"
TIMEOUT = 15
remessa = 000
def log(msg):
    print(f"[DEBUG] {msg}")

def safe_click(driver, by_locator,nome_elemento="Elemento", timeout=10):
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable(by_locator)
        )
        element.click()
        log(f"Clique padrão realizado com sucesso no elemento:{nome_elemento}")
    except (ElementClickInterceptedException, TimeoutException) as e:
        log(f"Clique padrão falhou: {repr(e)}. Tentando com JavaScript...")
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located(by_locator)
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", element)
            log("Clique forçado via JavaScript realizado com sucesso.")
        except Exception as js_e:
            log(f"Erro ao clicar com JavaScript: {repr(js_e)}")
    except Exception as e:
        log(f"Erro inesperado ao tentar clicar: {repr(e)}")

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


def selecionar_unidade_embarcadora(driver, item_embarcadora="M431"):
    for tentativa in range(3):
        try:
            log(f"[SGR] Tentando selecionar unidade embarcadora: {item_embarcadora} (tentativa {tentativa+1})")
            dropdown = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'ui selection dropdown')]"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", dropdown)
            dropdown.click()
            time.sleep(0.5)
            input_element = dropdown.find_element(By.XPATH, ".//input[@class='search']")
            input_element.clear()
            input_element.send_keys(item_embarcadora)
            time.sleep(0.5)
            input_element.send_keys(Keys.ENTER)

            log(f"[OK] Unidade embarcadora '{item_embarcadora}' selecionada com ENTER.")
            return True
        except Exception as e:
            log(f"[Tentativa {tentativa + 1}] Erro ao selecionar unidade embarcadora: {e}")
            time.sleep(2)
    log("[ERRO] Não foi possível selecionar a unidade embarcadora após várias tentativas.")
    return False


def preencher_datas_e_executar(driver, dias_passado=2, dias_futuro=1):
    try:
        hoje = datetime.date.today()
        data_final = hoje + datetime.timedelta(days=dias_futuro)
        data_inicial = hoje - datetime.timedelta(days=dias_passado)

        driver.find_element(By.ID, "D1").send_keys(data_inicial.strftime("%d/%m/%Y"))
        driver.find_element(By.ID, "D2").send_keys(data_final.strftime("%d/%m/%Y"))
        driver.find_element(By.ID, "BTN_EXECUTAR").click()
        time.sleep(5)
        log("Datas inseridas e botão 'Executar' clicado.")
    except Exception as e:
        log(f"[ERRO] Falha ao interagir com elementos de datas: {e}")


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

def _snapshot_downloads(download_dir: str) -> set[str]:
    try:
        return {f for f in os.listdir(download_dir)}
    except Exception:
        return set()

def _esperar_novo_arquivo(download_dir: str,
                          antes: set[str],
                          timeout: int = 240,
                          estabilizacao_seg: float = 1.2) -> str | None:

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



def baixar_relatorios_mais_recentes(driver, destino_dir=None, timeout_status=120):
    if destino_dir is None:
        destino_dir = fonte_dir  
    if not destino_dir or not os.path.isdir(destino_dir):
        log(f"[ERRO] Pasta de destino inválida: {destino_dir}")
        return False

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

        return True


    relatorios_desejados = ["Rastreabilidade", "Histórico Transações", "Estoque Detalhado"]

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

    safe_click(driver,(By.XPATH, "//a[@class='logo' and @ui-sref='home']"),"Botão Home")
    time.sleep(5)
    return True

if __name__ == "__main__":
    if not fonte_dir:
        log("[ERRO] Pasta OneDrive não encontrada por encontrar_pasta_onedrive_empresa().")
        raise SystemExit(1)
    os.makedirs(fonte_dir, exist_ok=True)

    edge_options = EdgeOptions()
    edge_options.use_chromium = True
    edge_options.add_argument("--start-maximized")
    edge_options.add_argument("--disable-dev-shm-usage")
    edge_options.add_argument("--no-sandbox")
    edge_options.debugger_address = "127.0.0.1:9222"

    service = EdgeService(executable_path="C://Users//xql80316//Downloads//edgedriver_win64//msedgedriver.exe")

    driver = None
    try:
        driver = webdriver.Edge(service=service, options=edge_options)
        log("Página carregada.")

        sucesso = interacoes_sgr(driver)
        if not sucesso:
            log("[FALHA] Não foi possível acessar a tela de Rastreabilidade.")
            raise SystemExit(1)

        log("[OK] Relatórios solicitados com sucesso. Aguardando processamento...")
        time.sleep(10)  

        ok = baixar_relatorios_mais_recentes(driver, destino_dir=fonte_dir)
        if ok:
            log("[OK] Downloads concluídos e salvos no OneDrive.")
        else:
            log("[ERRO] Falha no fluxo de downloads.")
    except Exception as e:
        log(f"[FATAL] Erro no main: {e}")
        raise
    finally:
        try:
            if driver is not None:
                driver.quit()
        except Exception:
            pass




