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

URL = "https://wmdsweb-dev1digital.mdb.com.br/"
USUARIO = "xql80316"
SENHA = "8583@Doug"
ITEM_FILIAL = "M431 - Divisao Vitarella - Logistico"
ITEM_DEPOSITO = "LA01"
TIMEOUT = 15
remessa = 000
def log(msg):
    print(f"[DEBUG] {msg}")

def dump_inputs_on_context(ctx, label, max_show=20):
    try:
        inputs = ctx.find_elements(By.CSS_SELECTOR, "input")
        log(f"[DUMP] {label}: {len(inputs)} inputs encontrados")
        for i, el in enumerate(inputs[:max_show], 1):
            try:
                _id = el.get_attribute("id")
                _name = el.get_attribute("name")
                _type = el.get_attribute("type")
                _ph = el.get_attribute("placeholder")
                _cls = el.get_attribute("class")
                _vis = el.is_displayed()
                log(f"   {i:02d}. id={_id} name={_name} type={_type} ph={_ph} class={_cls} vis={_vis}")
            except Exception as e:
                log(f"   {i:02d}. <erro ao inspecionar input> {e}")
    except Exception as e:
        log(f"[DUMP] Falha ao listar inputs em {label}: {e}")

def click_possible_login_button(driver):
    labels = ["Entrar", "Login", "Sign in", "Acessar"]
    for lbl in labels:
        try:
            btn = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((
                By.XPATH,
                f"//button[contains(@class,'v-btn') or self::button][.//span[normalize-space()='{lbl}'] or normalize-space()='{lbl}']"
            )))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            try:
                btn.click()
            except Exception:
                driver.execute_script("arguments[0].click();", btn)
            log(f"[LOGIN] Botão '{lbl}' clicado.")
            time.sleep(0.8)
            return True
        except Exception:
            continue
    return False

def set_input_value_vuetify_js(driver, input_css, value):
    js = """
    const selector = arguments[0];
    const val = arguments[1];
    const nodes = document.querySelectorAll(selector);
    let ok = false;
    nodes.forEach(n => {
        try {
            n.value = val;
            n.dispatchEvent(new Event('input', {bubbles:true}));
            n.dispatchEvent(new Event('change', {bubbles:true}));
            ok = true;
        } catch(e){}
    });
    return ok;
    """
    return driver.execute_script(js, input_css, value)

def tentar_login(driver, usuario, senha, timeout=15, log=print):
    log(f"[NAV] URL atual: {driver.current_url}")
    time.sleep(1.0)
    click_possible_login_button(driver)
    dump_inputs_on_context(driver, "MAIN")

    try:
        login_el = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input#LOGIN"))
        )
    except TimeoutException:
        log("[LOGIN] input#LOGIN não encontrado.")
        return False

    try:
        container = login_el.find_element(By.XPATH, "./ancestor::*[contains(@class,'v-field')][1]")
    except Exception:
        container = login_el
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", container)
    try:
        ActionChains(driver).move_to_element(container).pause(0.05).click(container).perform()
    except Exception:
        driver.execute_script("arguments[0].click();", container)
    time.sleep(0.2)

    try:
        if login_el.is_displayed() and login_el.is_enabled():
            login_el.clear()
            login_el.send_keys(usuario)
            log("[LOGIN] Usuário preenchido (send_keys).")
        else:
            ok_js = set_input_value_vuetify_js(driver, "input#LOGIN", usuario)
            log(f"[LOGIN] Usuário preenchido (JS) ok={ok_js}.")
    except Exception:
        ok_js = set_input_value_vuetify_js(driver, "input#LOGIN", usuario)
        log(f"[LOGIN] Usuário preenchido (JS fallback) ok={ok_js}.")

    senha_el = None
    try:
        senha_el = driver.find_element(By.CSS_SELECTOR, "input#SENHA")
    except Exception:
        pass
    if not senha_el:
        cands = driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
        if cands:
            senha_el = cands[0]

    if senha_el:
        try:
            senha_container = senha_el.find_element(By.XPATH, "./ancestor::*[contains(@class,'v-field')][1]")
        except Exception:
            senha_container = senha_el
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", senha_container)
        try:
            ActionChains(driver).move_to_element(senha_container).pause(0.05).click(senha_container).perform()
        except Exception:
            driver.execute_script("arguments[0].click();", senha_container)
        time.sleep(0.2)

        try:
            if senha_el.is_displayed() and senha_el.is_enabled():
                senha_el.clear()
                senha_el.send_keys(senha)
                log("[LOGIN] Senha preenchida (send_keys).")
            else:
                ok_js = set_input_value_vuetify_js(driver, "input#SENHA", senha)
                log(f"[LOGIN] Senha preenchida (JS) ok={ok_js}.")
        except Exception:
            ok_js = set_input_value_vuetify_js(driver, "input#SENHA", senha)
            log(f"[LOGIN] Senha preenchida (JS fallback) ok={ok_js}.")
    else:
        ok_js = set_input_value_vuetify_js(driver, "input[type='password']", senha)
        log(f"[LOGIN] Senha preenchida (JS sem id) ok={ok_js}.")

    try:
        if senha_el and senha_el.is_displayed() and senha_el.is_enabled():
            senha_el.send_keys(Keys.ENTER)
            log("[LOGIN] Enter enviado.")
        else:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ENTER)
            log("[LOGIN] Enter enviado no body.")
    except Exception:
        pass

    try:
        botao = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//button[contains(@class,'v-btn')][.//span[normalize-space()='Entrar'] or normalize-space()='Entrar']"
            ))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", botao)
        try:
            botao.click()
        except Exception:
            driver.execute_script("arguments[0].click();", botao)
        log("[LOGIN] Botão 'Entrar' clicado (fallback).")
    except Exception:
        pass

    try:
        WebDriverWait(driver, 15).until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(2)
    except Exception:
        pass
    log("[LOGIN] Fluxo de login finalizado (verifique se autenticou).")
    return True

def abrir_vuetify_dropdown(driver, field_id):
    try:
        field_container = WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, f"//input[@id='{field_id}']/ancestor::*[contains(@class,'v-field')][1]"))
        )
    except TimeoutException:
        log(f"[ERRO] Campo '{field_id}' não encontrado.")
        return
    try:
        icon = field_container.find_element(By.CSS_SELECTOR, ".v-autocomplete__menu-icon, .mdi-menu-down, .v-input__append-inner")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", icon)
        try:
            icon.click()
        except Exception:
            driver.execute_script("arguments[0].click();", icon)
        log(f"[OK] Ícone de dropdown '{field_id}' clicado.")
    except Exception:
        try:
            input_el = field_container.find_element(By.CSS_SELECTOR, "input")
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", input_el)
            input_el.click()
            log(f"[INFO] Ícone não encontrado. Input '{field_id}' clicado diretamente.")
        except Exception as e:
            log(f"[ERRO] Falha ao clicar no input '{field_id}': {e}")

def preencher_campo_autocomplete(driver, input_css, texto_input, texto_para_selecionar, timeout=10, partial=True):
    try:
        campo = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, input_css))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", campo)
        campo.clear()
        campo.send_keys(texto_input)
        time.sleep(1.2)
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_elements(By.XPATH, "//div[contains(@class,'v-list-item')]")) > 0
        )

        return selecionar_item_lista(driver, texto_para_selecionar, partial=partial)

    except Exception as e:
        log(f"[ERRO] Falha ao preencher campo '{input_css}': {e}")
        return False

def selecionar_item_lista(driver, valor_procurado, partial=True, timeout=10):
    log(f"[DEBUG] Procurando item na lista: '{valor_procurado}'")

    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_elements(By.XPATH, "//div[contains(@class,'v-list-item')]")) > 0
        )
    except:
        log("[ERRO] Nenhum item da lista visível encontrado.")
        return False

    itens = driver.find_elements(By.XPATH, "//div[contains(@class,'v-list-item')]")
    log(f"[DEBUG] {len(itens)} itens encontrados na lista.")

    for item in itens:
        texto = item.text.strip().lower()
        if (partial and valor_procurado.lower() in texto) or (not partial and valor_procurado.lower() == texto):
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", item)
                time.sleep(0.3)
                item.click()
                log(f"[OK] Item '{valor_procurado}' selecionado.")
                return True
            except Exception as e:
                log(f"[ERRO] Falha ao clicar no item: {e}")
                return False

    log(f"[ERRO] Item '{valor_procurado}' não encontrado entre os {len(itens)} visíveis.")
    return False
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

def realizar_login_e_selecao(driver, usuario, senha, item_filial, item_deposito):
    log("Iniciando login e seleção...")
    tentar_login(driver, usuario, senha, timeout=20, log=log)
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input#FILIAL"))
        )
        log("[PÓS-LOGIN] Campo FILIAL disponível.")
    except TimeoutException:
        log("[ERRO] Campo FILIAL não apareceu após o login.")
        return False

    time.sleep(1.5)

    ok_filial = preencher_campo_autocomplete(
        driver,
        input_css="input#FILIAL",
        texto_input=item_filial[:4], 
        texto_para_selecionar=item_filial
    )

    ok_deposito = preencher_campo_autocomplete(
        driver,
        input_css="input#DEPOSITO",
        texto_input=item_deposito,
        texto_para_selecionar=item_deposito
    )

    if not ok_filial or not ok_deposito:
        log("[ERRO] Falha ao selecionar FILIAL ou DEPOSITO.")
        return False

    log("[OK] FILIAL e DEPOSITO preenchidos com sucesso.")
    safe_click(
        driver,
        (By.ID, "BOTAO_SELECIONAR_LOCAL"),
        nome_elemento="Botão Selecionar"
    )
    if not esperar_tela_principal(driver):
        return False

    return True
def exportar_relatório(driver):
    try:
        safe_click(driver,
                (By.ID, "BOTAO_MENU"),
                nome_elemento="Botão de Menu"
                )
    except Exception as e:
        log("Click no menu não foi concluído")
    
    safe_click(
        driver,
        (By.XPATH, "//a[contains(@href, '/conferencias') and .//div[text()='Conferências']]"),
        nome_elemento="Item de Menu - Conferências"
    )
    safe_click(
        driver,
        (By.ID,"BOTAO_EXIBIR_FILTROS"),
        nome_elemento="Filtros"
    )

    hoje = datetime.date.today()
    data_inicial = hoje - datetime.timedelta(days=5)
    data_formatada = data_inicial.strftime("%d/%m/%Y")

    try:
        ok = set_input_value_vuetify_js(driver, "#input-19", data_formatada)
        log(f"[DATA] Data inicial preenchida via JS: {data_formatada} (ok={ok})")

    except Exception as e:
        log(f"[ERRO] Falha ao preencher data: {e}")
def esperar_tela_principal(driver, timeout=20):
    try:
        log("[ESPERA] Aguardando document.readyState == 'complete'...")
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        log("[ESPERA] Página carregada.")

        log("[ESPERA] Aguardando sumiço de overlays/spinners...")
        WebDriverWait(driver, timeout).until_not(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".v-overlay--active, .v-progress-circular"))
        )
        log("[ESPERA] Overlays e spinners sumiram.")

        log("[ESPERA] Aguardando botão BOTÃO_MENU aparecer...")
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.ID, "BOTAO_MENU"))
        )
        log("[PÓS-LOGIN] Tela principal totalmente carregada.")
        return True

    except TimeoutException as e:
        log("[ERRO] A tela principal não carregou completamente.")
        return False

if __name__ == "__main__":
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options = Options()
    chrome_options.debugger_address = "127.0.0.1:9222"
    service = Service(executable_path="C://Users//xql80316//Downloads//chromedriver-win64//chromedriver-win64//chromedriver.exe")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    #driver.get(URL)
    log("Página carregada.")

    sucesso = realizar_login_e_selecao(
        driver=driver,
        usuario=USUARIO,
        senha=SENHA,
        item_filial=ITEM_FILIAL,
        item_deposito=ITEM_DEPOSITO
    )
    if not sucesso:
        driver.quit()
        exit(1)
    exportar_relatório(driver)
