"""
descargar_historicos.py

Automates the download of historical meter data from the Coordinador
Electrico Nacional portal (https://medidas.coordinador.cl/reportes/).

The portal organizes data by year, month, and substation letter, packaged
as ZIP files containing dozens of CSVs covering the entire national grid.
This script navigates the dynamic JavaScript interface using Playwright,
downloads only the relevant ZIP files, filters the 8 target meters in memory,
and discards everything else.

Usage:
- Set ANIO, MES, and SUBESTACIONES before running
- Known substations for target meters: C, D, L, M, P, S
- Run in batches of 2-3 letters to avoid memory issues
- Output: inyeccion_{ANIO}_{MES}_parte_{letras}.csv

Note: Requires Playwright with Firefox installed.
Run 'playwright install firefox' before first use.
"""

from playwright.sync_api import sync_playwright
import pandas as pd
import zipfile
import io
import os
import gc

MEDIDORES = [
    'CABRERO_023_PMGD7_QTC',
    'CDNCARMN_220_J1_ECM',
    'LSANGLES_013_PMGD7_LAP',
    'LSANGLES_013_PMGD8_CCC',
    'MARELENA_220_JT1_GSS',
    'PARRONAL_013_PMGD2_HUE',
    'SCLMENTE_013_PMGD4_CFL',
    'SOLRJAMA_220_J1_RUC'
]

ANIO = "2025"
MES = "01"

# Change this variable each run. Known substations: C, D, L, M, P, S
SUBESTACIONES = list("CD")

CARPETA_DESCARGA = "./descargas_temp"
os.makedirs(CARPETA_DESCARGA, exist_ok=True)

def procesar_zip(ruta_zip):
    resultados = []
    with zipfile.ZipFile(ruta_zip, 'r') as z:
        for nombre_csv in z.namelist():
            if not nombre_csv.endswith('.csv'):
                continue
            with z.open(nombre_csv) as f:
                contenido = f.read()
                df = pd.read_csv(io.BytesIO(contenido), sep=';', encoding='utf-8-sig', dtype=str)
                df.columns = df.columns.str.strip()
                df['PUNTO DE MEDIDA'] = df['PUNTO DE MEDIDA'].str.strip()
                df_filtrado = df[df['PUNTO DE MEDIDA'].isin(MEDIDORES)].copy()
                if len(df_filtrado) > 0:
                    print(f"    Found {len(df_filtrado)} records in {nombre_csv}")
                    resultados.append(df_filtrado)
                del df, df_filtrado
                gc.collect()
    return resultados

with sync_playwright() as p:
    browser = p.firefox.launch(headless=True, downloads_path=CARPETA_DESCARGA)
    page = browser.new_page(accept_downloads=True)
    page.goto("https://medidas.coordinador.cl/reportes/")
    page.wait_for_timeout(5000)

    page.evaluate(f"document.getElementById('{ANIO}').querySelector('i.jstree-ocl').click();")
    page.wait_for_timeout(3000)
    page.evaluate(f"document.querySelector('[id=\"/{ANIO}/{MES}\"]').querySelector('i.jstree-ocl').click();")
    page.wait_for_timeout(3000)
    page.evaluate(f"document.querySelector('[id=\"//{ANIO}/{MES}/15MIN\"]').querySelector('i.jstree-ocl').click();")
    page.wait_for_timeout(3000)
    page.evaluate(f"document.querySelector('[id=\"/{ANIO}/{MES}/15MIN/csv\"]').querySelector('i.jstree-ocl').click();")
    page.wait_for_timeout(5000)

    todos_resultados = []

    for letra in SUBESTACIONES:
        zip_id = f"SUBESTACION_{letra}_15MIN_csv.zip"
        print(f"\nDownloading {zip_id}...")

        try:
            with page.expect_download(timeout=60000) as download_info:
                page.evaluate(f"document.getElementById('{zip_id}').querySelector('a.jstree-anchor').click();")

            download = download_info.value
            ruta_zip = os.path.join(CARPETA_DESCARGA, download.suggested_filename)
            download.save_as(ruta_zip)
            print(f"  Downloaded, processing...")

            resultados = procesar_zip(ruta_zip)
            todos_resultados.extend(resultados)

            os.remove(ruta_zip)
            gc.collect()

        except Exception as e:
            print(f"  Error in {zip_id}: {e}")
            continue

    browser.close()

if todos_resultados:
    df_raw = pd.concat(todos_resultados, ignore_index=True)

    df_procesado = pd.DataFrame()
    df_procesado['fecha_str'] = (
        df_raw['AÑO'].str.replace('\ufeff', '').str.strip().str.zfill(4) + '-' +
        df_raw['MES'].str.strip().str.zfill(2) + '-' +
        df_raw['DIA'].str.strip().str.zfill(2)
    )
    df_procesado['fecha'] = pd.to_datetime(df_procesado['fecha_str'], errors='coerce')
    df_procesado['hora'] = (
        df_raw['HORA'].str.strip().str.zfill(2) + ':' +
        df_raw['INICIO INTERVALO'].str.strip().str.zfill(2) + ':00'
    )
    df_procesado['medidor'] = df_raw['PUNTO DE MEDIDA'].str.strip()

    es_san_pedro = df_raw['PUNTO DE MEDIDA'].str.strip() == 'SOLRJAMA_220_J1_RUC'
    inyeccion_str = df_raw['Inyección_Energia_Activa (kWhR)'].str.strip().str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    retiro_str = df_raw['Retiro_Energia_Activa (kWhD)'].str.strip().str.replace('.', '', regex=False).str.replace(',', '.', regex=False)

    inyeccion_kwh = pd.to_numeric(inyeccion_str, errors='coerce')
    retiro_kwh = pd.to_numeric(retiro_str, errors='coerce')

    df_procesado['inyeccion_mwh'] = retiro_kwh.where(es_san_pedro, inyeccion_kwh) / 1000

    df_final = df_procesado[['fecha', 'hora', 'medidor', 'inyeccion_mwh']].dropna(subset=['fecha'])

    letras = "".join(SUBESTACIONES)
    archivo_salida = f'inyeccion_{ANIO}_{MES}_parte_{letras}.csv'
    df_final.to_csv(archivo_salida, index=False)

    print(f"\nCompleted: {ANIO}/{MES} substations {letras}")
    print(f"Total records: {len(df_final)}")
    print(f"Meters found: {df_final['medidor'].unique().tolist()}")
    print(f"Saved: {archivo_salida}")
else:
    print(f"No data found in substations {''.join(SUBESTACIONES)}")
