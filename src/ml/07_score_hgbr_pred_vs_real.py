# src/ml/07_score_hgbr_pred_vs_real.py
# Genera un parquet "pred vs real" para BI, sin reventar RAM:
# - Lee por batches desde DuckDB
# - Carga el modelo (soporta que joblib devuelva dict)
# - Alinea features con las del entrenamiento si están guardadas
# - Guarda parquet final con y_true, y_pred, abs_error y flags

from pathlib import Path
import numpy as np
import pandas as pd
import duckdb
import joblib
import pyarrow as pa
import pyarrow.parquet as pq


def find_project_root() -> Path:
    p = Path.cwd().resolve()
    for _ in range(8):
        if (p / "data").exists() or (p / ".git").exists():
            return p
        p = p.parent
    return Path.cwd().resolve()


def load_model_any(path: Path):
    obj = joblib.load(path)

    if hasattr(obj, "predict"):
        return obj, None  # (model, feature_list)

    if isinstance(obj, dict):
        # Candidatos típicos
        for k in ["model", "estimator", "pipeline", "regressor", "clf"]:
            if k in obj and hasattr(obj[k], "predict"):
                feats = obj.get("features") or obj.get("feature_cols") or obj.get("columns")
                return obj[k], feats

        # Si el dict solo tiene 1 valor y ese es un modelo
        if len(obj) == 1:
            v = list(obj.values())[0]
            if hasattr(v, "predict"):
                return v, None

        raise TypeError(f"El joblib contiene un dict pero no encuentro una clave con un modelo: {list(obj.keys())}")

    raise TypeError(f"Objeto cargado no soportado: {type(obj)}")


def main():
    ROOT = find_project_root()

    INP = ROOT / "data" / "gold" / "bicing_gold_ml_features_tplus1.parquet"
    MODEL_PATH = ROOT / "models" / "hgbr_tplus1.joblib"

    OUT_DIR = ROOT / "data" / "gold" / "bi"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT = OUT_DIR / "ml_pred_vs_real_last90d.parquet"

    if not INP.exists():
        raise FileNotFoundError(f"No existe: {INP}")
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"No existe: {MODEL_PATH}")

    print("ROOT :", ROOT)
    print("INP  :", INP)
    print("MODEL:", MODEL_PATH)
    print("OUT  :", OUT)

    model, saved_features = load_model_any(MODEL_PATH)
    print("Modelo cargado:", type(model))
    if saved_features is not None:
        print("✅ El joblib trae lista de features guardadas:", len(saved_features))

    TARGET = "y_bikes_tplus1"
    TIME_COL = "time_hour"

    # Columnas que queremos exportar a BI
    KEEP_COLS = [
        "station_id",
        "time_hour",
        TARGET,
        "bikes_available_mean",
        "docks_available_mean",
        "mechanical_mean",
        "ebike_mean",
        "obs_count",
        "hour",
        "dayofweek",
        "month",
        "is_weekend",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "pressure_msl",
        "lag_1h_bikes",
        "lag_2h_bikes",
        "lag_24h_bikes",
        "roll3h_bikes_mean",
        "holiday_any",
        "is_holiday_spain",
        "is_holiday_catalunya",
        "is_holiday_barcelona",
        "is_heavy_rain",
        "is_windy",
    ]

    # Drop para X
    DROP_FOR_X = {"date", "holiday_scope_final", "holiday_name"}  # por si aparecen
    NON_NUMERIC_DROP = {"time_hour"}  # en tu entrenamiento quitaste time_hour

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    # 1) max time
    q_max = f"SELECT MAX({TIME_COL}) AS max_t FROM read_parquet('{INP.as_posix()}')"
    max_t = con.execute(q_max).fetchone()[0]
    if max_t is None:
        raise RuntimeError("No se pudo leer max(time_hour).")
    max_t = pd.to_datetime(max_t)
    min_t = max_t - pd.Timedelta(days=90)
    print(f"Rango scoring: {min_t} -> {max_t}")

    # 2) columnas existentes
    df_one = con.execute(f"SELECT * FROM read_parquet('{INP.as_posix()}') LIMIT 1").df()
    cols_all = list(df_one.columns)

    keep_cols = [c for c in KEEP_COLS if c in cols_all]
    if TARGET not in keep_cols and TARGET in cols_all:
        keep_cols.append(TARGET)
    if TIME_COL not in keep_cols and TIME_COL in cols_all:
        keep_cols.append(TIME_COL)

    # feature_cols: si el modelo trae lista, úsala; si no, deriva
    if saved_features is not None:
        feature_cols = [c for c in saved_features if c in cols_all]
    else:
        feature_cols = [c for c in cols_all if c not in {TARGET} and c not in DROP_FOR_X]
        feature_cols = [c for c in feature_cols if c not in NON_NUMERIC_DROP]

    # 3) conteo
    q_count = f"""
    SELECT COUNT(*)
    FROM read_parquet('{INP.as_posix()}')
    WHERE {TIME_COL} >= TIMESTAMP '{min_t.strftime("%Y-%m-%d %H:%M:%S")}'
    """
    n_rows = con.execute(q_count).fetchone()[0]
    print("keep_cols:", len(keep_cols))
    print("feature_cols:", len(feature_cols))
    print("rows_to_score:", n_rows)

    # 4) streaming
    batch_size = 250_000
    writer = None
    total = 0

    select_cols = []
    # evita duplicados en SELECT
    for c in keep_cols + feature_cols:
        if c in cols_all and c not in select_cols:
            select_cols.append(c)

    query = f"""
    SELECT {", ".join([f'"{c}"' for c in select_cols])}
    FROM read_parquet('{INP.as_posix()}')
    WHERE {TIME_COL} >= TIMESTAMP '{min_t.strftime("%Y-%m-%d %H:%M:%S")}'
    ORDER BY {TIME_COL}
    """

    cur = con.execute(query)

    while True:
        df = cur.fetch_df_chunk(batch_size)
        if df is None or len(df) == 0:
            break

        if TIME_COL in df.columns:
            df[TIME_COL] = pd.to_datetime(df[TIME_COL], errors="coerce")

        # X con columnas en el orden esperado
        X = df[[c for c in feature_cols if c in df.columns]].copy()

        # quita no-numéricas
        non_numeric = [c for c in X.columns if not pd.api.types.is_numeric_dtype(X[c])]
        if non_numeric:
            X = X.drop(columns=non_numeric)

        # imputación por batch
        for c in X.columns:
            if X[c].isna().any():
                X[c] = X[c].fillna(X[c].median())

        # pred
        y_pred = model.predict(X)

        df_out = df[[c for c in keep_cols if c in df.columns]].copy()
        if TARGET not in df_out.columns:
            raise KeyError(f"No está el target {TARGET} en el batch. ¿Seguro que existe en el parquet?")

        df_out["y_pred"] = y_pred
        df_out["abs_error"] = np.abs(df_out[TARGET].astype(float) - df_out["y_pred"].astype(float))

        ordered = []
        for c in ["station_id", "time_hour", TARGET, "y_pred", "abs_error"]:
            if c in df_out.columns:
                ordered.append(c)
        for c in df_out.columns:
            if c not in ordered:
                ordered.append(c)
        df_out = df_out[ordered]

        table = pa.Table.from_pandas(df_out, preserve_index=False)

        if writer is None:
            writer = pq.ParquetWriter(OUT.as_posix(), table.schema, compression="snappy")
        writer.write_table(table)

        total += len(df_out)
        print(f"  batch -> {len(df_out):,} | total={total:,}")

    if writer is not None:
        writer.close()

    con.close()

    print("\n✅ OK. Creado:")
    print(OUT)
    print("rows_written:", total)


if __name__ == "__main__":
    main()
