#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_meteo_meteocat_barcelona.py
===================================
Descarga datos meteorológicos de Barcelona usando la API REST oficial del Servei Meteorològic de Catalunya.

API Meteocat:
  - Documentación: https://apidocs.meteocat.gencat.cat/
  - Estaciones:    GET https://api.meteo.cat/xema/v1/estacions
  - Variables:     GET https://api.meteo.cat/xema/v1/variables
  - Medidas:       GET https://api.meteo.cat/xema/v1/estacions/{codi}/variables/mesurades/{any}
  
Estaciones XEMA Barcelona (códigos oficiales):
  - X2: Barcelona - Zoo
  - X4: Barcelona - el Raval  
  - X8: Barcelona - Zona Universitària
  - D5: Barcelona - Observatori Fabra
  
Variables principales:
  - 32: Temperatura (°C)
  - 33: Humedad relativa (%)
  - 34: Precipitación (mm)
  - 35: Presión atmosférica (hPa)
  - 30: Velocidad del viento (m/s)

Estructura final del Parquet:
  - codi_estacio: str
  - codi_variable: int
  - data: datetime64[ns]
  - valor: float
  - codi_estat: str
  - nom_variable: str
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
import time

import requests
import pandas as pd
from tqdm import tqdm

# ==========================
# CONFIGURACIÓN
# ==========================

# API Meteocat
BASE_URL = "https://api.meteo.cat/xema/v1"

# Estaciones de Barcelona (códigos XEMA)
BARCELONA_STATIONS = {
    "X2": "Barcelona - Zoo",
    "X4": "Barcelona - el Raval",
    "X8": "Barcelona - Zona Universitària",
    "D5": "Barcelona - Observatori Fabra"
}

# Variables que queremos descargar
# Mapeo: código variable -> nombre
VARIABLES_MAP = {
    32: "Temperatura",      # °C
    33: "Humedad relativa", # %
    34: "Precipitación",    # mm
    35: "Presión",          # hPa
    30: "Vel. viento"       # m/s
}

# Rango de años a descargar (desde 2023)
START_YEAR = 2023
END_YEAR = 2025  # Hasta 2025 inclusive

# Ruta de salida
OUT_FILE = Path(__file__).parent.parent.parent / "data" / "raw" / "meteo_meteocat.parquet"

# ==========================
# FUNCIONES
# ==========================

def get_meteocat_api(endpoint: str, params: dict = None) -> dict:
    """
    Realiza una petición GET a la API de Meteocat.
    
    Args:
        endpoint: Endpoint de la API (ej. '/estacions')
        params: Parámetros de la query string
        
    Returns:
        Respuesta JSON parseada
    """
    url = f"{BASE_URL}{endpoint}"
    
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"❌ Error en petición a {url}: {e}")
        return None


def get_station_info() -> pd.DataFrame:
    """
    Obtiene información de todas las estaciones XEMA.
    
    Returns:
        DataFrame con información de estaciones
    """
    print("📡 Descargando información de estaciones...")
    data = get_meteocat_api("/estacions")
    
    if not data:
        return pd.DataFrame()
    
    # Convertir a DataFrame
    df = pd.DataFrame(data)
    
    # Filtrar solo estaciones de Barcelona
    df_bcn = df[df['codi'].isin(BARCELONA_STATIONS.keys())].copy()
    
    return df_bcn


def get_variable_info() -> pd.DataFrame:
    """
    Obtiene información de todas las variables disponibles.
    
    Returns:
        DataFrame con información de variables
    """
    print("📊 Descargando catálogo de variables...")
    data = get_meteocat_api("/variables")
    
    if not data:
        return pd.DataFrame()
    
    # Convertir a DataFrame
    df = pd.DataFrame(data)
    
    return df


def download_station_year_data(codi_estacio: str, year: int) -> pd.DataFrame:
    """
    Descarga datos de una estación para un año específico.
    
    Args:
        codi_estacio: Código de la estación (ej. 'X4')
        year: Año a descargar (ej. 2023)
        
    Returns:
        DataFrame con las medidas del año
    """
    endpoint = f"/estacions/{codi_estacio}/variables/mesurades/{year}"
    
    print(f"  📥 {codi_estacio} - {year}...", end=" ")
    
    data = get_meteocat_api(endpoint)
    
    if not data:
        print("❌ Sin datos")
        return pd.DataFrame()
    
    # La API devuelve una lista de variables, cada una con sus lecturas
    records = []
    
    for variable_data in data:
        codi_variable = variable_data.get('codi')
        lectures = variable_data.get('lectures', [])
        
        # Filtrar solo las variables que nos interesan
        if codi_variable not in VARIABLES_MAP:
            continue
        
        for lectura in lectures:
            records.append({
                'codi_estacio': codi_estacio,
                'codi_variable': codi_variable,
                'data': lectura.get('data'),
                'valor': lectura.get('valor'),
                'codi_estat': lectura.get('estat'),
                'codi_base': lectura.get('baseHoraria')
            })
    
    df = pd.DataFrame(records)
    
    if not df.empty:
        print(f"✅ {len(df):,} lecturas")
    else:
        print("⚠️ 0 lecturas")
    
    return df


def download_all_data() -> pd.DataFrame:
    """
    Descarga todos los datos de todas las estaciones y años.
    
    Returns:
        DataFrame consolidado con todos los datos
    """
    all_data = []
    
    total_combinations = len(BARCELONA_STATIONS) * (END_YEAR - START_YEAR + 1)
    
    print(f"\n🚀 Descargando datos de {len(BARCELONA_STATIONS)} estaciones × {END_YEAR - START_YEAR + 1} años = {total_combinations} combinaciones")
    print("="*60)
    
    for codi_estacio in BARCELONA_STATIONS.keys():
        station_name = BARCELONA_STATIONS[codi_estacio]
        print(f"\n📍 Estación: {codi_estacio} ({station_name})")
        
        for year in range(START_YEAR, END_YEAR + 1):
            df_year = download_station_year_data(codi_estacio, year)
            
            if not df_year.empty:
                all_data.append(df_year)
            
            # Rate limiting: esperar 0.5 segundos entre peticiones
            time.sleep(0.5)
    
    print("\n" + "="*60)
    
    if not all_data:
        print("❌ No se descargaron datos")
        return pd.DataFrame()
    
    # Concatenar todos los DataFrames
    df = pd.concat(all_data, ignore_index=True)
    
    return df


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa y limpia los datos descargados.
    
    Args:
        df: DataFrame con datos crudos
        
    Returns:
        DataFrame procesado
    """
    print("\n🔧 Post-procesamiento de datos...")
    
    # Convertir tipos
    df['codi_variable'] = pd.to_numeric(df['codi_variable'], errors='coerce').astype('Int64')
    df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
    
    # Convertir fecha
    df['data'] = pd.to_datetime(df['data'], errors='coerce')
    
    # Añadir nombre de variable
    df['nom_variable'] = df['codi_variable'].map(VARIABLES_MAP).fillna("UNKNOWN")
    
    # Filtrar solo datos válidos (estado 'V')
    df = df[df['codi_estat'] == 'V'].copy()
    
    # Eliminar nulos
    df = df.dropna(subset=['data', 'valor'])
    
    # Ordenar
    df = df.sort_values(['codi_estacio', 'codi_variable', 'data']).reset_index(drop=True)
    
    print(f"  ✅ {len(df):,} registros válidos después del procesamiento")
    
    return df


def main():
    """Función principal"""
    
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    print("\n" + "="*60)
    print("METEOCAT API - DESCARGA METEOROLÓGICA BARCELONA")
    print("="*60)
    
    # 1. Información de estaciones
    df_stations = get_station_info()
    
    if not df_stations.empty:
        print(f"\n✅ Estaciones encontradas: {len(df_stations)}")
        for _, row in df_stations.iterrows():
            print(f"   • {row['codi']}: {row.get('nom', 'N/A')}")
    
    # 2. Información de variables
    df_vars = get_variable_info()
    
    if not df_vars.empty:
        print(f"\n✅ Variables disponibles: {len(df_vars)}")
        # Mostrar solo las que nos interesan
        for code, name in VARIABLES_MAP.items():
            var_info = df_vars[df_vars['codi'] == code]
            if not var_info.empty:
                print(f"   • {code}: {name} ({var_info.iloc[0].get('unitat', 'N/A')})")
    
    # 3. Descargar datos
    df = download_all_data()
    
    if df.empty:
        print("\n❌ No se descargaron datos. Abortando.")
        return
    
    # 4. Procesar datos
    df = process_data(df)
    
    if df.empty:
        print("\n❌ No hay datos válidos después del procesamiento. Abortando.")
        return
    
    # 5. Guardar
    print(f"\n💾 Guardando resultados en {OUT_FILE}...")
    df.to_parquet(OUT_FILE, index=False)
    
    # 6. Resumen final
    print("\n" + "="*60)
    print(f"✅ COMPLETADO -> {OUT_FILE.resolve()}")
    print("="*60)
    print(f"📊 Total registros: {len(df):,}")
    print(f"📍 Estaciones: {df['codi_estacio'].nunique()} → {sorted(df['codi_estacio'].unique())}")
    print(f"📈 Variables: {df['codi_variable'].nunique()} → {sorted(df['codi_variable'].unique())}")
    print(f"📅 Rango fechas: {df['data'].min()} → {df['data'].max()}")
    
    print("\n📋 Preview:")
    print(df.head(10).to_string(index=False))
    
    print("\n📊 Registros por estación:")
    print(df.groupby('codi_estacio').size().to_string())
    
    print("\n📊 Registros por variable:")
    var_counts = df.groupby(['codi_variable', 'nom_variable']).size()
    print(var_counts.to_string())
    
    print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    main()