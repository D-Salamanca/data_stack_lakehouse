#!/usr/bin/env python3
"""
Script para configurar Dremio con fuentes de datos (Nessie y MinIO)
"""
import requests
import time
import sys
import json
import os
from urllib.parse import urljoin

# Configuración
# Usa el nombre del servicio si está en Docker, sino localhost
DREMIO_HOST = os.getenv("DREMIO_HOST", "proyecto2-dremio")
DREMIO_PORT = os.getenv("DREMIO_PORT", "9047")
DREMIO_URL = f"http://{DREMIO_HOST}:{DREMIO_PORT}"
DREMIO_USER = "admin"
DREMIO_PASS = "admin@123"

# Headers para requests
HEADERS = {"Content-Type": "application/json"}

def log(msg, level="INFO"):
    """Log con prefijo"""
    prefix = f"[{level}]" if level != "INFO" else "[✅]"
    print(f"{prefix} {msg}")

def wait_for_dremio(max_retries=30):
    """Espera a que Dremio esté disponible"""
    log("Esperando a que Dremio esté disponible...")
    for i in range(max_retries):
        try:
            resp = requests.get(urljoin(DREMIO_URL, "/api/v3/config"), timeout=5)
            if resp.status_code == 200:
                log("Dremio está disponible")
                return True
        except Exception as e:
            log(f"Intento {i+1}/{max_retries}: {e}", "WAIT")
            time.sleep(2)
    
    log("Dremio no respondió en tiempo límite", "ERROR")
    return False

def create_admin_user():
    """Crea el usuario administrador"""
    log("Creando usuario administrador...")
    
    payload = {
        "userName": DREMIO_USER,
        "password": DREMIO_PASS,
        "firstName": "Admin",
        "lastName": "User",
        "email": "admin@dremio.local"
    }
    
    try:
        resp = requests.post(
            urljoin(DREMIO_URL, "/api/v3/user"),
            json=payload,
            headers=HEADERS,
            timeout=10
        )
        
        if resp.status_code in [201, 200]:
            log(f"Usuario admin creado: {resp.status_code}")
            return True
        elif resp.status_code == 400:
            log("Usuario admin ya existe (probablemente)", "WARN")
            return True
        else:
            log(f"Error inesperado: {resp.status_code} - {resp.text}", "ERROR")
            return False
    except Exception as e:
        log(f"Excepción al crear usuario: {e}", "ERROR")
        return False

def get_auth_token():
    """Obtiene token de autenticación"""
    log("Obteniendo token de autenticación...")
    
    payload = {
        "userName": DREMIO_USER,
        "password": DREMIO_PASS
    }
    
    try:
        resp = requests.post(
            urljoin(DREMIO_URL, "/api/v3/login"),
            json=payload,
            headers=HEADERS,
            timeout=10
        )
        
        if resp.status_code == 200:
            token = resp.json().get("token")
            log(f"Token obtenido")
            return token
        else:
            log(f"Error al obtener token: {resp.status_code} - {resp.text}", "ERROR")
            return None
    except Exception as e:
        log(f"Excepción al obtener token: {e}", "ERROR")
        return None

def add_nessie_source(token):
    """Agrega Nessie como fuente de datos"""
    log("Agregando fuente Nessie...")
    
    auth_header = {**HEADERS, "Authorization": f"Bearer {token}"}
    
    payload = {
        "name": "nessie",
        "type": "NESSIE",
        "config": {
            "nessieEndpoint": "http://proyecto2-nessie:19120/api/v2",
            "authentication": {
                "authenticationType": "NONE"
            }
        }
    }
    
    try:
        resp = requests.post(
            urljoin(DREMIO_URL, "/api/v3/catalog"),
            json=payload,
            headers=auth_header,
            timeout=10
        )
        
        if resp.status_code in [201, 200]:
            log(f"Fuente Nessie creada: {resp.status_code}")
            return True
        elif resp.status_code == 400 and "already exists" in resp.text.lower():
            log("Fuente Nessie ya existe", "WARN")
            return True
        else:
            log(f"Error al crear Nessie: {resp.status_code} - {resp.text}", "ERROR")
            return False
    except Exception as e:
        log(f"Excepción al crear Nessie: {e}", "ERROR")
        return False

def add_minio_source(token):
    """Agrega MinIO como fuente de datos S3"""
    log("Agregando fuente MinIO (S3)...")
    
    auth_header = {**HEADERS, "Authorization": f"Bearer {token}"}
    
    payload = {
        "name": "minio",
        "type": "S3",
        "config": {
            "accessKey": "minioadmin",
            "secretKey": "minioadmin123",
            "rootPath": "s3a://iceberg",
            "propertyList": [
                {
                    "name": "fs.s3a.endpoint",
                    "value": "http://proyecto2-minio:9000"
                },
                {
                    "name": "fs.s3a.path.style.access",
                    "value": "true"
                },
                {
                    "name": "fs.s3a.aws.credentials.provider",
                    "value": "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider"
                }
            ]
        }
    }
    
    try:
        resp = requests.post(
            urljoin(DREMIO_URL, "/api/v3/catalog"),
            json=payload,
            headers=auth_header,
            timeout=10
        )
        
        if resp.status_code in [201, 200]:
            log(f"Fuente MinIO creada: {resp.status_code}")
            return True
        elif resp.status_code == 400 and "already exists" in resp.text.lower():
            log("Fuente MinIO ya existe", "WARN")
            return True
        else:
            log(f"Error al crear MinIO: {resp.status_code} - {resp.text}", "ERROR")
            return False
    except Exception as e:
        log(f"Excepción al crear MinIO: {e}", "ERROR")
        return False

def list_sources(token):
    """Lista las fuentes de datos disponibles"""
    log("Listando fuentes de datos...")
    
    auth_header = {**HEADERS, "Authorization": f"Bearer {token}"}
    
    try:
        resp = requests.get(
            urljoin(DREMIO_URL, "/api/v3/catalog"),
            headers=auth_header,
            timeout=10
        )
        
        if resp.status_code == 200:
            sources = resp.json()
            log(f"Fuentes disponibles:")
            for src in sources.get("data", []):
                log(f"  - {src.get('name')} ({src.get('type')})")
            return True
        else:
            log(f"Error al listar fuentes: {resp.status_code}", "ERROR")
            return False
    except Exception as e:
        log(f"Excepción al listar fuentes: {e}", "ERROR")
        return False

def main():
    """Función principal"""
    log("=== Configuración de Dremio ===")
    
    # 1. Esperar a que Dremio esté disponible
    if not wait_for_dremio():
        sys.exit(1)
    
    # 2. Crear usuario admin
    if not create_admin_user():
        log("Continuando con configuración...")
    
    time.sleep(2)
    
    # 3. Obtener token
    token = get_auth_token()
    if not token:
        log("No se pudo obtener token de autenticación", "ERROR")
        sys.exit(1)
    
    time.sleep(2)
    
    # 4. Agregar fuentes
    nessie_ok = add_nessie_source(token)
    time.sleep(2)
    minio_ok = add_minio_source(token)
    time.sleep(2)
    
    # 5. Listar fuentes
    list_sources(token)
    
    # Resumen
    log("=== Configuración completada ===")
    if nessie_ok and minio_ok:
        log("✅ Dremio configurado exitosamente")
        log(f"Accede a: {DREMIO_URL}")
        log(f"Usuario: {DREMIO_USER}")
        sys.exit(0)
    else:
        log("⚠️ Configuración completada con advertencias", "WARN")
        sys.exit(0)

if __name__ == "__main__":
    main()
