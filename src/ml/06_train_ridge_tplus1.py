# 06_train_ridge_tplus1.py
# Ridge con split temporal y entrenamiento eficiente usando DuckDB + sample determinista.
# Evita cargar 20M filas en pandas.
#
# Salidas:
# - reports/tables/ml_ridge_metrics.txt
# - reports/tables/ml_ridge_segment_mae.csv
# - models/ridge_tplus1.joblib

from pathlib import Path
import numpy as np
import pandas as pd
import joblib
import duckdb

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error


def find_project_root() -> Path:
    p = Path.cwd().resolve()
    for _ in range(8):
        if (p / "data").exists() or (p / ".git").exists():
            return p
        p = p.parent
    return Path.cwd().resolve()


def rmse(y_true, y_pred) -> float:
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


def main():
    ROOT = find_project_root()

    INP = ROOT / "data" / "gold" / "bicing_gold_ml_features_tplus1.parquet"
    if not INP.exists():
        raise FileNotFoundError(f"No existe el input: {INP}")

    TARGET = "y_bikes_tplus1"
    TIME_COL = "time_hour"

    # Tamaños de sample (ajusta según tu RAM/tiempo)
    TRAIN_N = 3_000_000   # 3M
    TEST_N  = 1_000_000   # 1M

    RIDGE_ALPHA = 1.0

    print("ROOT:", ROOT)
    print("INP :", INP)

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=8;")

    # 1) Columnas disponibles
    desc = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{INP.as_posix()}')"
    ).df()
    cols = desc["column_name"].tolist()

    if TARGET not in cols:
        raise KeyError(
            f"Falta TARGET={TARGET}. Columnas candidatas: "
            f"{[c for c in cols if 'y_' in c.lower() or 'tplus' in c.lower()]}"
        )
    if TIME_COL not in cols:
        raise KeyError(f"Falta TIME_COL={TIME_COL}")

    # Excluir strings/categóricas si existen
    DROP_COLS = {"holiday_scope_final", "holiday_name", "date"}
    DROP_FEATURES = set(DROP_COLS) | {TIME_COL, TARGET}

    feature_cols = [c for c in cols if c not in DROP_FEATURES]

    # 2) Calcular cut_time temporal (80%)
    cut_time = con.execute(f"""
        SELECT quantile(time_hour, 0.8) AS cut_time
        FROM read_parquet('{INP.as_posix()}')
        WHERE time_hour IS NOT NULL AND {TARGET} IS NOT NULL
    """).fetchone()[0]

    print("cut_time (80%):", cut_time)

    # 3) Sample determinista
    # DuckDB antiguo no soporta random(seed), así que ordenamos por un hash determinista.
    # Con esto, el sample es estable entre ejecuciones.
    order_expr = "hash(station_id, CAST(time_hour AS VARCHAR))"

    select_cols = ", ".join(feature_cols + [TARGET, "holiday_any", "is_heavy_rain", "is_weekend"])

    train_q = f"""
        SELECT {select_cols}
        FROM read_parquet('{INP.as_posix()}')
        WHERE time_hour < TIMESTAMP '{cut_time}'
          AND {TARGET} IS NOT NULL
        ORDER BY {order_expr}
        LIMIT {TRAIN_N}
    """
    test_q = f"""
        SELECT {select_cols}
        FROM read_parquet('{INP.as_posix()}')
        WHERE time_hour >= TIMESTAMP '{cut_time}'
          AND {TARGET} IS NOT NULL
        ORDER BY {order_expr}
        LIMIT {TEST_N}
    """

    print(f"Extrayendo train sample: {TRAIN_N:,} filas ...")
    train_df = con.execute(train_q).df()
    print(f"Extrayendo test sample : {TEST_N:,} filas ...")
    test_df = con.execute(test_q).df()
    con.close()

    print("train shape:", train_df.shape, "| test shape:", test_df.shape)

    # 4) Separar X/y
    X_train = train_df[feature_cols].copy()
    y_train = train_df[TARGET].copy()
    X_test = test_df[feature_cols].copy()
    y_test = test_df[TARGET].copy()

    non_numeric = [c for c in X_train.columns if not pd.api.types.is_numeric_dtype(X_train[c])]
    if non_numeric:
        print("⚠️ Quitando columnas no numéricas:", non_numeric)
        X_train = X_train.drop(columns=non_numeric)
        X_test = X_test.drop(columns=non_numeric)
        feature_cols = [c for c in feature_cols if c not in non_numeric]

    # 5) Pipeline Ridge
    model = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ("ridge", Ridge(alpha=RIDGE_ALPHA))
    ])

    print("Entrenando Ridge...")
    model.fit(X_train, y_train)
    pred = model.predict(X_test)

    mae = float(mean_absolute_error(y_test, pred))
    _rmse = rmse(y_test, pred)

    print("\nRidge -> MAE:", mae, "| RMSE:", _rmse)

    # 6) Segmentos
    seg_cols = ["holiday_any", "is_heavy_rain", "is_weekend"]
    rows = []
    for col in seg_cols:
        if col not in test_df.columns:
            continue
        vals = sorted([v for v in test_df[col].dropna().unique()])
        for v in vals:
            m = test_df[col] == v
            if int(m.sum()) == 0:
                continue
            seg_mae = float(mean_absolute_error(y_test[m], pred[m]))
            rows.append({
                "segment": col,
                "value": int(v) if float(v).is_integer() else float(v),
                "n_rows": int(m.sum()),
                "mae": seg_mae
            })
    seg_out = pd.DataFrame(rows).sort_values(["segment", "value"]) if rows else pd.DataFrame()

    # 7) Guardados
    (ROOT / "reports" / "tables").mkdir(parents=True, exist_ok=True)
    (ROOT / "models").mkdir(parents=True, exist_ok=True)

    out_metrics = ROOT / "reports" / "tables" / "ml_ridge_metrics.txt"
    out_metrics.write_text(
        f"MODEL=Ridge\n"
        f"MAE={mae}\nRMSE={_rmse}\n"
        f"cut_time={cut_time}\n"
        f"train_sample={len(train_df)}\n"
        f"test_sample={len(test_df)}\n"
        f"n_features={len(feature_cols)}\n"
        f"alpha={RIDGE_ALPHA}\n"
    )
    print("✅ Métricas guardadas:", out_metrics)

    out_seg = ROOT / "reports" / "tables" / "ml_ridge_segment_mae.csv"
    if not seg_out.empty:
        seg_out.to_csv(out_seg, index=False)
        print("✅ Segmentos guardados:", out_seg)
    else:
        print("ℹ️ Segmentos: no se generó tabla.")

    out_model = ROOT / "models" / "ridge_tplus1.joblib"
    joblib.dump(model, out_model)
    print("✅ Modelo guardado:", out_model)


if __name__ == "__main__":
    main()
