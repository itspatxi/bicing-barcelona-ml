cat > src/ml/06_train_hgbr_tplus1_sample.py << 'EOF'
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.ensemble import HistGradientBoostingRegressor
import joblib

def find_root():
    p = Path.cwd().resolve()
    for _ in range(8):
        if (p / "src").exists() and (p / "data").exists():
            return p
        p = p.parent
    return Path.cwd().resolve()

def main():
    ROOT = find_root()

    INP = ROOT / "data" / "gold" / "samples" / "bicing_gold_ml_features_tplus1_sample_1M.parquet"
    if not INP.exists():
        raise FileNotFoundError(f"No existe el sample: {INP}")

    TARGET = "y_bikes_tplus1"
    TIME_COL = "time_hour"

    print("ROOT:", ROOT)
    print("INP :", INP)

    df = pq.read_table(INP).to_pandas()

    # Tipos
    if TIME_COL in df.columns:
        df[TIME_COL] = pd.to_datetime(df[TIME_COL], errors="coerce")

    if TARGET not in df.columns:
        raise KeyError(f"Falta target {TARGET}. Columnas disponibles: {list(df.columns)}")

    df = df.dropna(subset=[TARGET]).copy()

    # Split temporal (80/20)
    if TIME_COL in df.columns:
        df = df.sort_values(TIME_COL)
        cut_idx = int(len(df) * 0.8)
        cut_time = df.iloc[cut_idx][TIME_COL]
        train = df[df[TIME_COL] < cut_time].copy()
        test  = df[df[TIME_COL] >= cut_time].copy()
        print("Split temporal:")
        print(" - train rows:", len(train), "| hasta:", train[TIME_COL].max())
        print(" - test  rows:", len(test),  "| desde:", test[TIME_COL].min())
    else:
        # fallback
        train = df.sample(frac=0.8, random_state=42)
        test = df.drop(train.index)

    # Features: todo menos target y no-numéricas
    drop_cols = {TARGET}
    X_train = train.drop(columns=[c for c in drop_cols if c in train.columns])
    y_train = train[TARGET].copy()

    X_test  = test.drop(columns=[c for c in drop_cols if c in test.columns])
    y_test  = test[TARGET].copy()

    # quitar no numéricas
    non_num = [c for c in X_train.columns if not pd.api.types.is_numeric_dtype(X_train[c])]
    if non_num:
        print("Quitando no numéricas:", non_num)
        X_train = X_train.drop(columns=non_num)
        X_test  = X_test.drop(columns=non_num)

    # imputación simple
    for c in X_train.columns:
        med = X_train[c].median()
        X_train[c] = X_train[c].fillna(med)
        X_test[c]  = X_test[c].fillna(med)

    print("X_train:", X_train.shape, "| X_test:", X_test.shape)

    model = HistGradientBoostingRegressor(
        loss="squared_error",
        learning_rate=0.08,
        max_iter=300,
        min_samples_leaf=50,
        random_state=42
    )
    model.fit(X_train, y_train)
    pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, pred)
    rmse = np.sqrt(mean_squared_error(y_test, pred))
    print("HGBR(sample) -> MAE:", mae, "| RMSE:", rmse)

    # guardar
    out_dir = ROOT / "models"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_model = out_dir / "hgbr_tplus1_sample.joblib"
    joblib.dump({"model": model, "features": list(X_train.columns)}, out_model)
    print("✅ Modelo guardado:", out_model)

    out_metrics = ROOT / "reports" / "tables"
    out_metrics.mkdir(parents=True, exist_ok=True)
    (out_metrics / "ml_hgbr_sample_metrics.txt").write_text(f"MAE={mae}\nRMSE={rmse}\n")
    print("✅ Métricas guardadas:", out_metrics / "ml_hgbr_sample_metrics.txt")

if __name__ == "__main__":
    main()
EOF
