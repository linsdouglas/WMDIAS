import time
import datetime
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

def interacoes_sgr(driver, actions):
        driver.set_window_size(1920, 2000)
        try:
            log("[SGR] Aguardando menu 'Logística/Faturamento'...")
            menu_logistica = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='ui dropdown item' and contains(text(),'Logística/Faturamento')]"))
            )
            menu_logistica.click()
            log("[SGR] Menu 'Logística/Faturamento' clicado.")
        except Exception as e:
            log("[ERRO] Falha ao clicar em 'Logística/Faturamento'. Tentando via JS...")
            safe_click(driver, 
                    (By.XPATH, "//div[@class='ui dropdown item' and contains(text(),'Logística/Faturamento')]"), 
                    nome_elemento="Menu Logística")
            time.sleep(1)
        try:
            log("[SGR] Aguardando submenu 'Relatórios WMDiaS'...")
            submenu_wmdias = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='item ng-scope' and @alt='Relatórios WMDiaS']"))
            )
            submenu_wmdias.click()
            log("[SGR] Submenu 'Relatórios WMDiaS' clicado.")
        except Exception as e:
            log("[ERRO] Falha ao clicar em 'Relatórios WMDiaS'. Tentando via JS...")
            safe_click(driver, 
                    (By.XPATH, "//div[@class='item ng-scope' and @alt='Relatórios WMDiaS']"), 
                    nome_elemento="Submenu Relatórios WMDiaS")
            time.sleep(1)

        try:
            log("[SGR] Aguardando item final 'Rastreabilidade'...")
            rastreabilidade = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='item ng-scope' and @alt='Rastreabilidade']"))
            )
            rastreabilidade.click()
            log("[SGR] Item 'Rastreabilidade' clicado com sucesso.")
        except Exception as e:
            log("[ERRO] Falha ao clicar em 'Rastreabilidade'. Tentando via JS...")
            safe_click(driver, 
                    (By.XPATH, "//div[@class='item ng-scope' and @alt='Rastreabilidade']"), 
                    nome_elemento="Item Rastreabilidade")

        return True


if __name__ == "__main__":
    edge_options = EdgeOptions()
    edge_options.add_argument("--start-maximized")
    edge_options.add_argument("--disable-dev-shm-usage")
    edge_options.add_argument("--no-sandbox")
    edge_options.use_chromium = True
    edge_options.debugger_address = "127.0.0.1:9222"
    service = EdgeService(executable_path="C://Users//xql80316//Downloads//edgedriver_win64//msedgedriver.exe")
    driver = webdriver.Edge(service=service, options=edge_options)
    #driver.get(URL)
    log("Página carregada.")

    if not interacoes_sgr(driver):
        log("[FALHA] Não foi possível acessar a tela de Rastreabilidade.")


