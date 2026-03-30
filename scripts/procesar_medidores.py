"""
procesar_medidores.py

Processes raw 15-minute meter CSV files downloaded from the Coordinador
Electrico Nacional portal. Each file corresponds to one plant and contains
energy injection and withdrawal readings in kWh.

This script:
- Reads all CSV files in the current directory
- Handles BOM encoding and European number format (comma as decimal separator)
- Detects the Jama/San Pedro plant and inverts its columns (known data issue)
- Converts kWh to MWh
- Outputs a single consolidated CSV ready to load into MySQL

Usage: Run from the folder containing the raw meter CSV files.
Output: inyeccion_medidores_final.csv
"""

import pandas as pd
import os
import gc

carpeta_medidores = "."

archivos = [f for f in os.listdir(carpeta_medidores) if f.endswith('.csv')]
print(f"Archivos encontrados: {len(archivos)}")

dfs_medidores = []

for archivo in archivos:
    print(f"\nProcesando: {archivo}")
    ruta = os.path.join(carpeta_medidores, archivo)
    
    df = pd.read_csv(ruta, sep=';', encoding='utf-8-sig', dtype=str)
    df.columns = df.columns.str.strip()
    df['AÑO'] = df['AÑO'].str.replace('\ufeff', '').str.strip()
    
    df_procesado = pd.DataFrame()
    
    df_procesado['fecha_str'] = (
        df['AÑO'].str.zfill(4) + '-' +
        df['MES'].str.strip().str.zfill(2) + '-' +
        df['DIA'].str.strip().str.zfill(2)
    )
    df_procesado['fecha_str'] = df_procesado['fecha_str'].str.replace('\ufeff', '')
    df_procesado['fecha'] = pd.to_datetime(df_procesado['fecha_str'], errors='coerce')
    
    df_procesado['hora'] = (
        df['HORA'].str.strip().str.zfill(2) + ':' +
        df['INICIO INTERVALO'].str.strip().str.zfill(2) + ':00'
    )
    
    df_procesado['medidor'] = df['PUNTO DE MEDIDA'].str.strip()
    
    
    es_san_pedro = 'SAN PEDRO' in archivo.upper()
    
    if es_san_pedro:
        print(f"San Pedro detectado → invirtiendo columnas")
        col_usar = 'Retiro_Energia_Activa (kWhD)'
    else:
        col_usar = 'Inyección_Energia_Activa (kWhR)'
    
    inyeccion_str = df[col_usar].str.strip()
    inyeccion_str = inyeccion_str.str.replace('.', '', regex=False)
    inyeccion_str = inyeccion_str.str.replace(',', '.', regex=False)
    
    df_procesado['inyeccion_kwh'] = pd.to_numeric(inyeccion_str, errors='coerce')
    
    
    df_procesado['inyeccion_mwh'] = df_procesado['inyeccion_kwh'] / 1000
    
    
    df_final = df_procesado[['fecha', 'hora', 'medidor', 'inyeccion_mwh']].dropna(subset=['fecha'])
    
    con_datos = (df_final['inyeccion_mwh'] > 0).sum()
    con_cero = (df_final['inyeccion_mwh'] == 0).sum()
    con_null = df_final['inyeccion_mwh'].isna().sum()
    print(f"  ✓ Total: {len(df_final)} | Con datos: {con_datos} | Ceros: {con_cero} | NULL: {con_null}")
    
    dfs_medidores.append(df_final)
    del df, df_procesado, df_final
    gc.collect()

df_completo = pd.concat(dfs_medidores, ignore_index=True)
df_completo.to_csv('inyeccion_medidores_final.csv', index=False)

print(f"\n{'='*60}")
print(f"PROCESAMIENTO COMPLETADO")
print(f"   Total registros: {len(df_completo)}")
print(f"   Con datos > 0:   {(df_completo['inyeccion_mwh'] > 0).sum()}")
print(f"   Con cero:        {(df_completo['inyeccion_mwh'] == 0).sum()}")
print(f"   NULL:            {df_completo['inyeccion_mwh'].isna().sum()}")
print(f"\nPor medidor:")
for medidor in sorted(df_completo['medidor'].unique()):
    d = df_completo[df_completo['medidor'] == medidor]
    print(f"   {medidor}: datos={( d['inyeccion_mwh']>0).sum()} | ceros={(d['inyeccion_mwh']==0).sum()} | null={d['inyeccion_mwh'].isna().sum()}")
print(f"{'='*60}")
