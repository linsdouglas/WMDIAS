
import time
import math
import datetime
import threading
import tkinter as tk
from tkinter import messagebox, scrolledtext
from tkinter import ttk
import customtkinter as ctk
from PIL import Image
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException
import pyautogui as pt
import yagmail
from openpyxl import load_workbook
import comtypes.client
import glob
import shutil
import subprocess
import pandas as pd
import os
import sys
import win32print
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("green")
stop_event = threading.Event()

unidade="WMDIAS"
download_dir = os.path.join(os.environ['USERPROFILE'], 'Downloads', unidade)
if not os.path.exists(download_dir):
    os.makedirs(download_dir)
url = "https://wmdsweb-dev1digital.mdb.com.br/"
sumatra_path = os.path.join(os.environ['USERPROFILE'], "AppData", "Local", "SumatraPDF", "SumatraPDF.exe")
driver = None
loop_interval = 120
loop_count = 0

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920x1080")
prefs = {
    "download.default_directory":download_dir,
    "download.prompt_for_download": False,
    "directory_upgrade":True
}
chrome_options.add_experimental_option("prefs", prefs)

def log(msg):
    log_text.insert(tk.END, msg + "\n")
    log_text.see(tk.END)
    print(msg)

def login_WMDIAS():
    try:
        username_field=WebDriverWait(driver,5).until(
            EC.presence_of_element_located((By.XPATH,"//input[@type='text' and @name='LOGIN']"))
        )
        log("Campo de usuário encontrado, efetuando login...")
        username_field.clear()
        username_field.send_keys(global_username)
        password_field = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//input[@type='password' and @name='SENHA']"))
        )
        password_field.clear()
        password_field.send_keys(global_password)
        password_field.send_keys(Keys.ENTER)
        log("Login efetuado com sucesso.")
        time.sleep(5)
    except Exception as e:
        log("Login não requerido após refresh ou elemento não encontrado")

def safe_click(driver, by_locator, nome_elemento ="Elemento", timeout = 10):
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable(by_locator)
        )
        element.click()
        log(f"Clique padrão realizado com sucesso no elemento: {nome_elemento}")
    except (ElementClickInterceptedException, TimeoutException) as e:
        log(f"clique padrão falhou: {repr(e)}. Tentando com Javascript...")
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located(by_locator)
            )
            driver.execute_script

def iniciar_processo():
    global bg_thread, stop_event, global_username, global_password
    global_username = username_entry.get().strip()
    global_password = password_entry.get().strip()
# ----- INTERFACE GRÁFICA -----
janela = ctk.CTk()
janela.title("Gerador de Canhotos Automatizado - Paulista")
janela.geometry("450x550")
frame_status = ctk.CTkFrame(janela, fg_color="transparent")
frame_status.pack(pady=10, padx=10, fill="both", expand=True)
log_text = ctk.CTkTextbox(frame_status, width=400, height=200)
log_text.grid(row=1, column=0, padx=5, pady=5)
frame_login = ctk.CTkFrame(janela, fg_color ="transparent")
frame_login.pack(pady=10, padx=10, fill="x")
usuario_label = ctk.CTkLabel(frame_login, text="Usuário:", font=("Arial", 12))
usuario_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")
username_entry = ctk.CTkEntry(frame_login, width=200, placeholder_text="Digite seu usuário")
username_entry.grid(row=0, column=1, padx=5, pady=5)
password_entry = ctk.CTkEntry(frame_login, width=200, placeholder_text="Digite sua senha", show="*")
password_entry.grid(row=1, column=1, padx=5, pady=5)
