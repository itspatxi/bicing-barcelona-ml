# 🚴‍♂️ Bicing Barcelona — End-to-End Data & ML Project

**Autor:** Patxi BA  
**Fecha:** Febrero 2026  

Proyecto end-to-end de analítica y Machine Learning sobre el sistema de bicicletas compartidas **Bicing (Barcelona)**.  
Incluye pipeline de datos **Bronze/Silver/Gold**, EDA con visualizaciones, datasets para **Power BI / Microsoft Fabric**, y un modelo de ML para predicción **t+1**.

---

## 🎯 Objetivo

Construir un sistema completo que:
- Integra telemetría horaria de estaciones Bicing con **meteorología** y **festivos**
- Asegura calidad de datos con arquitectura **Medallion (Bronze/Silver/Gold)**
- Genera datasets listos para consumo:
  - **BI:** análisis operativo y segmentación por meteo/festivos
  - **ML:** features + target para forecasting t+1
- Entrena y evalúa modelos (baseline + ML) con validación temporal
- Publica un dataset de **scoring** (predicción vs real) para monitorización en Power BI

---

## 📦 Datos (resumen)

- **Volumen:** ~27.3M registros
- **Estaciones:** 564
- **Rango temporal:** 2019-03-24 → 2025-12-31
- **Granularidad:** 1 fila = (station_id, time_hour)

### Variables principales
- `bikes_available_mean`, `docks_available_mean`, `mechanical_mean`, `ebike_mean`
- Meteorología: `temperature_2m`, `precipitation`, `wind_speed_10m`, `pressure_msl`, `relative_humidity_2m`
- Calendario: `is_holiday_spain`, `is_holiday_catalunya`, `is_holiday_barcelona`, `holiday_any`

---

## 🧱 Arquitectura Medallion (Bronze / Silver / Gold)

### 🥉 Bronze (Raw)
Datos tal cual entran (CSV/JSON/ICS).  
Objetivo: **trazabilidad y reproducibilidad**.

### 🥈 Silver (Clean & Standardized)
Limpieza y estandarización:
- parsing robusto de timestamps
- tipos numéricos correctos
- eliminación de outliers evidentes (ej: fechas 1970)
- normalización de esquema (nombres consistentes)

Salida: Parquet limpio por fuente (Bicing / meteo / festivos).

### 🥇 Gold (Analytics Ready)
Integración y dataset final:
- joins por `time_hour` (meteo) y por `date` (festivos)
- deduplicación global por clave `(station_id, time_hour)` → **0 duplicados**
- features temporales + flags listas para BI/ML
- control de cobertura por estación (ruido vs series robustas)

---

## ✅ Datasets Gold generados

| Dataset | Ruta | Uso |
|---|---|---|
| Gold base enriquecido | `data/gold/bicing_gold_final_plus.parquet` | Base completa con meteo + festivos |
| Vista BI “Plus” | `data/gold/bicing_gold_bi_plus.parquet` | Power BI / Fabric con flags (`holiday_any`, `is_rain`, `is_windy`...) |
| Vista ML features + target | `data/gold/bicing_gold_ml_features_tplus1.parquet` | Entrenamiento ML (t+1) |
| Pred vs Real últimos 90d | `data/gold/bi/ml_pred_vs_real_last90d_plus.parquet` | Monitorización en BI |

---

## 🔍 EDA (Exploratory Data Analysis)

Análisis exploratorio con:
- Distribuciones (`hist`, percentiles)
- Patrones horarios y semanales (`line`, `bar`)
- Comparativas por festivo / lluvia / viento (`boxplot`)
- Cobertura por estación (histograma + ECDF)
- Correlaciones y señales útiles para features (lags y rolling)

**Hallazgo clave:** la señal más fuerte es **autoregresiva** (pasado reciente):
- `lag_1h_bikes`, `roll3h_bikes_mean` dominan correlación y poder predictivo  
Meteo y festivos aportan contexto, pero efecto medio menor.

---

## 🤖 Machine Learning (Forecast t+1)

### Problema
Regresión supervisada:
- **Target:** `y_bikes_tplus1` = bicicletas disponibles en la **hora siguiente**
- Validación: **split temporal** (sin leakage)

### Features (ejemplos)
- Lags: `lag_1h_bikes`, `lag_2h_bikes`, `lag_24h_bikes`
- Rolling: `roll3h_bikes_mean`
- Cíclicas: `sin_hour`, `cos_hour`, `sin_dow`, `cos_dow`
- Meteo: `temperature_2m`, `precipitation`, `wind_speed_10m`
- Flags: `holiday_any`, `is_rain`, `is_heavy_rain`, `is_windy`

### Modelos probados
- Naive baselines (lag/rolling)
- Ridge (baseline lineal)
- Random Forest
- HistGradientBoostingRegressor (mejor equilibrio)

**Mejor modelo (local):** HGBR  
- **MAE ~ 1.74**
- **RMSE ~ 2.60**

### Scoring para BI
Se genera un parquet con últimos ~90 días:
- `y_bikes_tplus1` (real)
- `y_pred` (predicción)
- `abs_error` (error absoluto)
y se usa en Power BI para monitorizar performance por estación/hora.

---

## 📊 Power BI + Microsoft Fabric

Se suben datasets Gold a Fabric (Lakehouse) y se construyen reportes con:
- KPIs operativos (bicis, docks, meteo, festivos)
- Segmentación por `holiday_any`, `is_rain`, `is_windy`
- Calidad de estaciones (coverage tags, sparse/noise)
- Predicción vs real (últimos 90 días, MAE/RMSE)


---

## 🛠️ Requisitos

- Python 3.10+ (recomendado)
- Dependencias principales:
  - `pandas`, `pyarrow`, `duckdb`
  - `matplotlib`
  - `scikit-learn`
  - `joblib`

Instalación típica:
```bash
pip install -r requirements.txt
