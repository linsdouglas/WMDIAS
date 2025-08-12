import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException

URL = "https://wmdsweb-dev1digital.mdb.com.br/"
USUARIO = "vit06329"
SENHA = "Ce112206@"
ITEM_FILIAL = "M431 - Divisao Vitarella - Logistico"
TIMEOUT = 15

def log(msg):
    print(f"[DEBUG] {msg}")

# ============ helpers genéricos ============
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
    """
    Seta o value e dispara eventos input/change para atualizar o v-model do Vuetify,
    mesmo se o input estiver invisível.
    """
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

    # Primeiro tenta encontrar o ícone clássico do Vuetify
    try:
        icon = field_container.find_element(By.CSS_SELECTOR, ".v-autocomplete__menu-icon, .mdi-menu-down, .v-input__append-inner")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", icon)
        try:
            icon.click()
        except Exception:
            driver.execute_script("arguments[0].click();", icon)
        log(f"[OK] Ícone de dropdown '{field_id}' clicado.")
    except Exception:
        # Se não tiver ícone, tenta clicar diretamente no input
        try:
            input_el = field_container.find_element(By.CSS_SELECTOR, "input")
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", input_el)
            input_el.click()
            log(f"[INFO] Ícone não encontrado. Input '{field_id}' clicado diretamente.")
        except Exception as e:
            log(f"[ERRO] Falha ao clicar no input '{field_id}': {e}")


def selecionar_item_vuetify(driver, valor, partial=False):
    overlay = WebDriverWait(driver, TIMEOUT).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, ".v-overlay.v-overlay--active .v-overlay__content"))
    )

    if partial:
        xpath_item = f".//div[contains(@class,'v-list-item-title')][contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), {repr(valor.lower())})]"
    else:
        xpath_item = f".//div[contains(@class,'v-list-item-title')][normalize-space()={repr(valor)}]"

    item_el = None
    for i in range(30):
        try:
            el = overlay.find_element(By.XPATH, xpath_item)
            if el.is_displayed():
                item_el = el
                break
        except Exception:
            pass

        # DEBUG: print itens visíveis
        try:
            itens_visiveis = overlay.find_elements(By.XPATH, ".//div[contains(@class,'v-list-item-title')]")
            log(f"[OVERLAY] Itens visíveis ({len(itens_visiveis)}):")
            for item in itens_visiveis:
                log("   - " + item.text.strip())
        except Exception as e:
            log(f"[OVERLAY] Erro ao listar itens: {e}")

        # Scrolla overlay
        try:
            scrollable = overlay.find_element(By.XPATH, ".//*[contains(@class,'v-virtual-scroll')]")
        except Exception:
            scrollable = overlay
        driver.execute_script("arguments[0].scrollTop += 240;", scrollable)
        time.sleep(0.2)

    if item_el is None:
        log(f"[Vuetify] Item '{valor}' não encontrado no overlay.")
        return False

    driver.execute_script("arguments[0].scrollIntoView({block:'nearest'});", item_el)
    try:
        item_el.click()
        log(f"Item '{valor}' selecionado.")
    except Exception:
        driver.execute_script("arguments[0].click();", item_el)
        log(f"Item '{valor}' selecionado via JS.")
    time.sleep(0.3)
    return True


if __name__ == "__main__":
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get(URL)
        log("Página carregada.")
        tentar_login(driver, USUARIO, SENHA, timeout=20, log=log)
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input#FILIAL"))
            )
            log("[PÓS-LOGIN] Campo FILIAL disponível.")
        except TimeoutException:
            log("[ERRO] Campo FILIAL não apareceu após o login.")
            driver.quit()
            exit(1)
        time.sleep(1.5)
        abrir_vuetify_dropdown(driver, "FILIAL")
        time.sleep(1)
        ok = selecionar_item_vuetify(driver, ITEM_FILIAL, partial=True)
        if not ok:
            log("Falha ao selecionar ITEM_FILIAL.")

        time.sleep(1)
        ok = selecionar_item_vuetify(driver, ITEM_FILIAL, partial=True)
        if not ok:
            log("Falha ao selecionar ITEM_FILIAL. Tente partial=True ou confirme o texto exato.")
        abrir_vuetify_dropdown(driver, "DEPOSITO")
        time.sleep(1)
        ok = selecionar_item_vuetify(driver, "LA01", partial=True)
        if not ok:
            log("Falha ao selecionar DEPOSITO. Tente partial=True ou confirme o texto exato.")


        log("Teste concluído. Veja no navegador se a seleção foi aplicada.")
        input("Pressione Enter para sair...")

    finally:
        driver.quit()