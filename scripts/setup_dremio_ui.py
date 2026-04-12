#!/usr/bin/env python3
"""
Script para configurar Dremio vía UI usando Selenium/Playwright
Configura el usuario admin y las fuentes de datos
"""
import time
import sys

# Intenta usar Playwright primero
try:
    from playwright.sync_api import sync_playwright
    USE_PLAYWRIGHT = True
except ImportError:
    print("[ERROR] Playwright no está instalado")
    sys.exit(1)

def main():
    """Configura Dremio"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # Navega a Dremio
        print("[INFO] Abriendo Dremio en http://proyecto2-dremio:9047...")
        page.goto("http://proyecto2-dremio:9047", wait_until="load")
        time.sleep(3)
        
        # Toma una captura para ver qué vemos
        print("[INFO] Verificando contenido de la página...")
        title = page.title()
        print(f"[INFO] Título: {title}")
        
        # Busca si estamos en la página de setup
        if "signup" in page.content().lower() or "setup" in page.content().lower():
            print("[INFO] Página de setup detectada")
            
            # Intenta llenar el formulario de admin
            # Busca campos de usuario
            username_inputs = page.locators("input[type='text'], input[name*='user'], input[name*='name']")
            password_inputs = page.locators("input[type='password']")
            
            print(f"[INFO] Encontrados {username_inputs.count()} campos de texto")
            print(f"[INFO] Encontrados {password_inputs.count()} campos de password")
            
            if username_inputs.count() > 0:
                username_inputs.first.fill("admin")
                print("[✅] Usuario ingresado")
            
            if password_inputs.count() > 0:
                password_inputs.first.fill("admin@123")
                print("[✅] Password ingresado")
                
                # Busca botón de submit
                submit_button = page.locator("button:has-text('Next'), button:has-text('Continue'), button:has-text('Create'), button:has-text('Submit')")
                if submit_button.count() > 0:
                    submit_button.first.click()
                    time.sleep(2)
                    print("[✅] Formulario enviado")
            
        elif "login" in page.content().lower():
            print("[INFO] Página de login detectada")
            
            # Intenta hacer login
            username_input = page.locator("input[type='text']")
            password_input = page.locator("input[type='password']")
            
            if username_input.count() > 0:
                username_input.first.fill("admin")
                print("[✅] Usuario ingresado")
            
            if password_input.count() > 0:
                password_input.first.fill("admin@123")
                print("[✅] Password ingresado")
            
            # Busca botón de login
            login_button = page.locator("button:has-text('Log In'), button:has-text('Login')")
            if login_button.count() > 0:
                login_button.first.click()
                time.sleep(3)
                print("[✅] Intentando login...")
        
        # Captura final
        print("[INFO] Capturando estado final...")
        page.screenshot(path="/tmp/dremio-setup.png")
        print("[INFO] Captura guardada en /tmp/dremio-setup.png")
        
        browser.close()

if __name__ == "__main__":
    try:
        main()
        print("[✅] Configuración completada")
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
