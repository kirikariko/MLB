#!/usr/bin/env python3
"""
ml_predict.py — MLB Syndicate ML Ensemble Predictor
====================================================
로컬에서 실행하여 ml_predictions.json 생성 후 GitHub push.
Claude 샌드박스 디스크 부족 문제 해결용.

사용법:
  pip install xgboost lightgbm scikit-learn shap pandas numpy
  python ml_predict.py --date 2026-05-16

입력:
  - data/mlb_two.csv (학습 데이터)
  - pipeline/{DATE}/dept1/raw_stats.json (오늘 경기 피처)
  - pipeline/{DATE}/dept1/environment.json (환경 데이터, optional)

출력:
  - pipeline/{DATE}/dept2/ml_predictions.json
"""

import argparse
import json
import os
import sys
import warnings
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    print("[WARN] xgboost not installed. pip install xgboost")

try:
    import lightgbm as lgb
    HAS_LGB = True
except ImportError:
    HAS_LGB = False
    print("[WARN] lightgbm not installed. pip install lightgbm")

try:
    import shap
    HAS_SHAP = True
except ImportError:
    HAS_SHAP = False
    print("[WARN] shap not installed. pip install shap (SHAP 설명 비활성)")

warnings.filterwarnings('ignore')

# ============================================================
# CONFIG
# ============================================================
BASE_DIR = Path(__file__).parent  # MLB 폴더 루트

# 97개 컬럼 중 학습에 사용할 피처 (타겟/메타 제외)
NON_FEATURE_COLS = [
    'Winning_Team', 'UO_RESULT', 'DATE', 'GAME_ID',
    'UMP_NAME', 'HOME_SIT_TAG', 'AWAY_SIT_TAG',
    'y_wl', 'y_ou',  # target columns — exclude from features
]

# ⛔ 절대 제거 금지 보호 피처 (CLAUDE.md 규칙)
PROTECTED_FEATURES = [
    'PARK_FACTOR', 'HOME_SP_IP', 'AWAY_SP_IP',
    'HOMETEAM_FPct', 'AWAYTEAM_FPct',
    'HOMETEAM_BULLPEN_WORKLOAD', 'AWAYTEAM_BULLPEN_WORKLOAD',
    'BVP_AVG_HOME', 'BVP_AVG_AWAY',
    'HOME_REST_DAYS', 'AWAY_REST_DAYS',
    'Series_Game_Num'
]

# NOTE: 이전에 있던 HOME_MAP/AWAY_MAP/SP_IP_MAP/ENV_MAP/META_MAP 5개 매핑은
# raw_stats.json -> CSV 컬럼 변환용이었으나, mlb_two.csv 단일 소스로 단순화하면서
# 불필요해져 제거됨. CSV 컬럼명이 그대로 피처명이므로 매핑 불필요.

# ============================================================
# GITHUB AUTO-DOWNLOAD
# ============================================================
GITHUB_REPO = 'kirikariko/MLB'
GITHUB_BRANCH = 'main'


def fetch_from_github(remote_path: str, local_path: Path, max_retries: int = 2) -> bool:
    """Sync a file from GitHub via `git fetch` + checkout (works with private repos).

    Strategy:
      1. Use git CLI (already authenticated) to fetch origin/{branch}
      2. Checkout `remote_path` from origin/{branch} into local_path
    Returns True on success, False on failure (caller should fallback to local cache).
    """
    import subprocess

    repo_root = Path(__file__).parent
    last_err = None
    for attempt in range(max_retries):
        try:
            # Fetch latest from remote (silent)
            r = subprocess.run(
                ['git', 'fetch', 'origin', GITHUB_BRANCH],
                cwd=str(repo_root), capture_output=True, text=True, timeout=30,
            )
            if r.returncode != 0:
                last_err = f"git fetch failed: {r.stderr.strip()}"
                continue

            # Checkout the file from origin/{branch} into local_path
            local_path.parent.mkdir(parents=True, exist_ok=True)
            # Get file contents from remote ref via git show
            r2 = subprocess.run(
                ['git', 'show', f'origin/{GITHUB_BRANCH}:{remote_path}'],
                cwd=str(repo_root), capture_output=True, timeout=30,
            )
            if r2.returncode != 0:
                last_err = f"git show failed: {r2.stderr.decode().strip()}"
                continue

            tmp = local_path.with_suffix(local_path.suffix + '.dl')
            tmp.write_bytes(r2.stdout)
            tmp.replace(local_path)
            print(f"  [GitHub] Synced {remote_path} ({len(r2.stdout)} bytes) -> {local_path}")
            return True
        except (subprocess.TimeoutExpired, OSError) as e:
            last_err = str(e)
            if attempt < max_retries - 1:
                import time as _t
                _t.sleep(1)
    print(f"  [GitHub] Failed to sync {remote_path}: {last_err}")
    return False


# ============================================================
# DATA LOADING
# ============================================================
def load_all_data(base_dir: Path, target_date: str = None):
    """mlb_two.csv 하나에서 학습 데이터 + 예측 데이터를 모두 로드.

    구조:
      - 결과 있는 행 (Winning_Team in '1'/'0') → 학습용
      - target_date의 결과 없는 행 → 예측 대상 (없으면 마지막 날짜)
    GitHub에서 최신본 자동 다운로드 (실패 시 로컬 캐시 fallback).
    """
    # GitHub 우선, 실패 시 로컬 fallback
    csv_local = base_dir / 'data' / 'mlb_two.csv'
    csv_root = base_dir / 'mlb_two.csv'
    print(f"[1/6] mlb_two.csv 로드")
    fetched = fetch_from_github('mlb_two.csv', csv_local)
    if not fetched:
        # GitHub 실패 — 로컬 캐시 사용
        if csv_local.exists():
            print(f"  [Fallback] Using cached {csv_local}")
        elif csv_root.exists():
            print(f"  [Fallback] Using {csv_root}")
            csv_local = csv_root
        else:
            print(f"  ⛔ HALT: GitHub 다운로드 실패 + 로컬 캐시 없음")
            sys.exit(1)

    df = pd.read_csv(csv_local)
    print(f"  전체 행: {len(df)}, 컬럼: {len(df.columns)}")

    # 예측 대상 결정 — target_date (있으면), 없으면 결과 비어있는 마지막 날짜
    df['DATE'] = df['DATE'].astype(str)
    no_result = df['Winning_Team'].isna() | ~df['Winning_Team'].astype(str).isin(['1', '0'])
    if target_date:
        predict_mask = (df['DATE'] == target_date) & no_result
    else:
        # 결과 없는 행 중 가장 최근 날짜
        pending = df[no_result]
        if pending.empty:
            print(f"  ⛔ HALT: 예측 대상 (결과 없는 행) 없음")
            sys.exit(1)
        target_date = pending['DATE'].max()
        predict_mask = (df['DATE'] == target_date) & no_result
    df_predict = df[predict_mask].copy()
    print(f"  예측 대상 ({target_date}): {len(df_predict)}경기")

    if df_predict.empty:
        print(f"  ⛔ HALT: {target_date}에 예측할 경기 없음")
        sys.exit(1)

    # 학습 데이터: W/L 결과 있는 행
    valid_wl = df['Winning_Team'].astype(str).isin(['1', '0']) & (df['DATE'] < target_date)
    df_wl = df[valid_wl].copy()
    df_wl['y_wl'] = df_wl['Winning_Team'].astype(int)
    print(f"  W/L 학습 행: {len(df_wl)} (target_date 이전, 결과 있음)")

    # 학습 데이터: O/U 결과 있는 행
    valid_ou = df['UO_RESULT'].astype(str).isin(['1', '0']) & (df['DATE'] < target_date)
    df_ou = df[valid_ou].copy()
    df_ou['y_ou'] = df_ou['UO_RESULT'].astype(int)
    print(f"  O/U 학습 행: {len(df_ou)}")

    return df_wl, df_ou, df_predict, target_date


# 하위 호환 (구 이름 유지)
def load_training_data(base_dir: Path):
    df_wl, df_ou, _, _ = load_all_data(base_dir)
    return df_wl, df_ou


def get_feature_cols(df: pd.DataFrame):
    """피처 컬럼 선택 — 95% null 필터 + 보호 피처"""
    candidates = [c for c in df.columns if c not in NON_FEATURE_COLS]

    # 숫자형만
    num_cols = []
    for c in candidates:
        try:
            pd.to_numeric(df[c], errors='raise')
            num_cols.append(c)
        except (ValueError, TypeError):
            # 숫자 변환 시도
            converted = pd.to_numeric(df[c], errors='coerce')
            if converted.notna().sum() > len(df) * 0.05:  # 5% 이상 유효
                num_cols.append(c)

    # 95% null 필터 (⛔ 50% 필터 사용 금지 — CLAUDE.md 규칙)
    filtered = []
    for c in num_cols:
        null_pct = pd.to_numeric(df[c], errors='coerce').isna().mean()
        if null_pct < 0.95:
            filtered.append(c)
        elif c in PROTECTED_FEATURES:
            # 보호 피처는 null 비율과 무관하게 포함
            filtered.append(c)
            print(f"  [보호] {c} (null {null_pct:.1%}) — 보호 피처로 유지")

    # 보호 피처 누락 확인
    for pf in PROTECTED_FEATURES:
        if pf in num_cols and pf not in filtered:
            filtered.append(pf)
            print(f"  [보호] {pf} 강제 추가")

    print(f"  피처 수: {len(filtered)} (95% null 필터 적용)")

    if len(filtered) < 30:
        print(f"  ⛔ HALT: 피처 {len(filtered)}개 < 30개 최소 기준")
        sys.exit(1)
    elif len(filtered) < 50:
        print(f"  ⚠️ WARN: 피처 {len(filtered)}개 < 50개 권장")

    return filtered


def prepare_features(df: pd.DataFrame, feature_cols: list):
    """피처 행렬 준비 — median imputation"""
    X = df[feature_cols].copy()
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors='coerce')

    # Median imputation (⛔ 0이나 mean 대치 금지 — CLAUDE.md 규칙)
    imputer = SimpleImputer(strategy='median')
    X_imputed = pd.DataFrame(
        imputer.fit_transform(X),
        columns=feature_cols,
        index=X.index
    )
    return X_imputed, imputer


# ============================================================
# MODEL TRAINING
# ============================================================
def train_wl_models(X: pd.DataFrame, y: pd.Series, feature_cols: list):
    """W/L 예측 모델 학습 — XGBoost + LightGBM + LogisticRegression"""
    print(f"\n[2/6] W/L 모델 학습 (n={len(X)}, features={len(feature_cols)})")

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    models = {}
    scores = {}

    # 1. Logistic Regression
    lr = LogisticRegression(max_iter=1000, C=0.1, random_state=42)
    cv_lr = cross_val_score(lr, X_scaled, y, cv=5, scoring='accuracy')
    lr.fit(X_scaled, y)
    models['lr'] = lr
    scores['lr'] = cv_lr.mean()
    print(f"  LR  CV accuracy: {cv_lr.mean():.4f} (+/- {cv_lr.std():.4f})")

    # 2. XGBoost
    if HAS_XGB:
        xgb_model = xgb.XGBClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            reg_alpha=0.1, reg_lambda=1.0,
            random_state=42, verbosity=0,
            eval_metric='logloss'
        )
        cv_xgb = cross_val_score(xgb_model, X, y, cv=5, scoring='accuracy')
        xgb_model.fit(X, y)
        models['xgb'] = xgb_model
        scores['xgb'] = cv_xgb.mean()
        print(f"  XGB CV accuracy: {cv_xgb.mean():.4f} (+/- {cv_xgb.std():.4f})")

    # 3. LightGBM
    if HAS_LGB:
        lgb_model = lgb.LGBMClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            reg_alpha=0.1, reg_lambda=1.0,
            random_state=42, verbose=-1
        )
        cv_lgb = cross_val_score(lgb_model, X, y, cv=5, scoring='accuracy')
        lgb_model.fit(X, y)
        models['lgb'] = lgb_model
        scores['lgb'] = cv_lgb.mean()
        print(f"  LGB CV accuracy: {cv_lgb.mean():.4f} (+/- {cv_lgb.std():.4f})")

    return models, scores, scaler


def train_ou_models(X: pd.DataFrame, y: pd.Series, feature_cols: list):
    """O/U 예측 모델 학습"""
    print(f"\n[3/6] O/U 모델 학습 (n={len(X)}, features={len(feature_cols)})")

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    models = {}
    scores = {}

    lr = LogisticRegression(max_iter=1000, C=0.1, random_state=42)
    cv_lr = cross_val_score(lr, X_scaled, y, cv=5, scoring='accuracy')
    lr.fit(X_scaled, y)
    models['lr'] = lr
    scores['lr'] = cv_lr.mean()
    print(f"  LR  CV accuracy: {cv_lr.mean():.4f} (+/- {cv_lr.std():.4f})")

    if HAS_XGB:
        xgb_model = xgb.XGBClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            random_state=42, verbosity=0, eval_metric='logloss'
        )
        cv_xgb = cross_val_score(xgb_model, X, y, cv=5, scoring='accuracy')
        xgb_model.fit(X, y)
        models['xgb'] = xgb_model
        scores['xgb'] = cv_xgb.mean()
        print(f"  XGB CV accuracy: {cv_xgb.mean():.4f} (+/- {cv_xgb.std():.4f})")

    if HAS_LGB:
        lgb_model = lgb.LGBMClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            random_state=42, verbose=-1
        )
        cv_lgb = cross_val_score(lgb_model, X, y, cv=5, scoring='accuracy')
        lgb_model.fit(X, y)
        models['lgb'] = lgb_model
        scores['lgb'] = cv_lgb.mean()
        print(f"  LGB CV accuracy: {cv_lgb.mean():.4f} (+/- {cv_lgb.std():.4f})")

    return models, scores, scaler


# ============================================================
# PREDICTION (single-CSV flow — see predict_from_df below)
# ============================================================


def _weighted_avg(preds: dict, scores: dict) -> float:
    """CV accuracy 기반 가중 평균"""
    total_w = 0
    total_p = 0
    for name, pred in preds.items():
        w = scores.get(name, 0.5)
        total_w += w
        total_p += pred * w
    return total_p / total_w if total_w > 0 else 0.5


def predict_from_df(df_predict: pd.DataFrame, feature_cols: list, imputer,
                    wl_models: dict, wl_scaler, wl_scores: dict,
                    ou_models: dict, ou_scaler, ou_scores: dict):
    """예측 — df_predict의 각 행을 그대로 피처로 사용 (raw_stats.json 매핑 불필요)."""
    results = []

    # 한 번에 변환 + imputation (배치 처리 가능하지만 행별 로직 유지)
    X_predict = df_predict[feature_cols].copy()
    for c in X_predict.columns:
        X_predict[c] = pd.to_numeric(X_predict[c], errors='coerce')
    X_imputed = pd.DataFrame(imputer.transform(X_predict), columns=feature_cols,
                             index=X_predict.index)

    X_wl_scaled = wl_scaler.transform(X_imputed)
    X_ou_scaled = ou_scaler.transform(X_imputed)

    for pos, (idx, _) in enumerate(df_predict.iterrows()):
        game_id = df_predict.loc[idx, 'GAME_ID']

        # 매핑률 = 원본 값이 null이 아닌 피처 비율
        raw_row = X_predict.loc[idx]
        mapped = int(raw_row.notna().sum())
        mapping_pct = mapped / len(feature_cols) * 100

        row_imputed = X_imputed.loc[[idx]]
        row_wl_scaled = X_wl_scaled[pos:pos + 1]
        row_ou_scaled = X_ou_scaled[pos:pos + 1]

        # W/L 예측
        wl_preds = {}
        if 'lr' in wl_models:
            wl_preds['lr'] = float(wl_models['lr'].predict_proba(row_wl_scaled)[0][1])
        if 'xgb' in wl_models:
            wl_preds['xgb'] = float(wl_models['xgb'].predict_proba(row_imputed)[0][1])
        if 'lgb' in wl_models:
            wl_preds['lgb'] = float(wl_models['lgb'].predict_proba(row_imputed)[0][1])
        wl_ensemble = _weighted_avg(wl_preds, wl_scores)

        # O/U 예측
        ou_preds = {}
        if 'lr' in ou_models:
            ou_preds['lr'] = float(ou_models['lr'].predict_proba(row_ou_scaled)[0][1])
        if 'xgb' in ou_models:
            ou_preds['xgb'] = float(ou_models['xgb'].predict_proba(row_imputed)[0][1])
        if 'lgb' in ou_models:
            ou_preds['lgb'] = float(ou_models['lgb'].predict_proba(row_imputed)[0][1])
        ou_ensemble = _weighted_avg(ou_preds, ou_scores)

        results.append({
            'game_id': game_id,
            'ml_home_win_prob': round(wl_ensemble, 4),
            'ml_away_win_prob': round(1.0 - wl_ensemble, 4),
            'ml_over_prob': round(ou_ensemble, 4),
            'ml_under_prob': round(1.0 - ou_ensemble, 4),
            'model_breakdown': {
                'wl': {k: round(v, 4) for k, v in wl_preds.items()},
                'ou': {k: round(v, 4) for k, v in ou_preds.items()},
            },
            'mapping_pct': round(mapping_pct, 1),
            'features_mapped': mapped,
            'features_total': len(feature_cols),
        })
        print(f"  {game_id}: WL={wl_ensemble:.3f}, OU={ou_ensemble:.3f}  mapping={mapping_pct:.0f}%")

    return results


def compute_shap_from_df(results: list, df_predict: pd.DataFrame, feature_cols: list,
                         imputer, wl_models: dict, ou_models: dict):
    """SHAP — DataFrame 기반."""
    if not HAS_SHAP:
        print("\n[5/6] SHAP 스킵 (shap 미설치)")
        return results

    print(f"\n[5/6] SHAP 계산")

    wl_model = wl_models.get('xgb') or wl_models.get('lgb')
    if wl_model is None:
        print("  SHAP 스킵 (tree 모델 없음)")
        return results

    try:
        explainer = shap.TreeExplainer(wl_model)

        X_predict = df_predict[feature_cols].copy()
        for c in X_predict.columns:
            X_predict[c] = pd.to_numeric(X_predict[c], errors='coerce')
        X_imputed = pd.DataFrame(imputer.transform(X_predict), columns=feature_cols)

        for i in range(len(df_predict)):
            row = X_imputed.iloc[[i]]
            shap_vals = explainer.shap_values(row)
            sv = shap_vals[1][0] if isinstance(shap_vals, list) else shap_vals[0]

            top_idx = np.argsort(np.abs(sv))[-10:][::-1]
            shap_features = [{
                'feature': feature_cols[idx],
                'shap_value': round(float(sv[idx]), 4),
                'feature_value': round(float(row.iloc[0, idx]), 4),
            } for idx in top_idx]

            results[i]['shap_features'] = shap_features

            fi = dict(zip(feature_cols, [round(float(x), 4) for x in np.abs(sv)]))
            results[i]['feature_importance'] = dict(sorted(fi.items(), key=lambda x: x[1], reverse=True)[:20])

        print(f"  SHAP 완료 ({len(df_predict)}경기)")
    except Exception as e:
        print(f"  SHAP 오류: {e}")

    return results


# ============================================================
# OUTPUT
# ============================================================
def save_output(results: list, date_str: str, base_dir: Path,
                wl_scores: dict, ou_scores: dict, n_features: int,
                n_train_wl: int, n_train_ou: int, avg_mapping_pct: float):
    """ml_predictions.json 저장"""
    print(f"\n[6/6] 출력 저장")

    out_dir = base_dir / 'pipeline' / date_str / 'dept2'
    out_dir.mkdir(parents=True, exist_ok=True)

    output = {
        'date': date_str,
        'model_status': 'active',
        'training_source': 'mlb_two.csv_direct',
        'training_rows_wl': n_train_wl,
        'training_rows_ou': n_train_ou,
        'n_features': n_features,
        'mapping_pct': round(avg_mapping_pct, 1),
        'models': {
            'wl': {k: round(v, 4) for k, v in wl_scores.items()},
            'ou': {k: round(v, 4) for k, v in ou_scores.items()},
        },
        'model_names': list(wl_scores.keys()),
        'ensemble_method': 'cv_accuracy_weighted',
        'null_threshold': 0.95,
        'imputation': 'median',
        'games': results,
    }

    out_path = out_dir / 'ml_predictions.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"  저장: {out_path}")
    print(f"  모델: {list(wl_scores.keys())}")
    print(f"  W/L CV: {', '.join(f'{k}={v:.4f}' for k,v in wl_scores.items())}")
    print(f"  O/U CV: {', '.join(f'{k}={v:.4f}' for k,v in ou_scores.items())}")
    print(f"  피처: {n_features}개, 매핑: {avg_mapping_pct:.1f}%")

    return out_path


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(description='MLB ML Ensemble Predictor')
    parser.add_argument('--date', type=str, default=None,
                        help='분석 날짜 (YYYY-MM-DD, 기본=오늘 EDT)')
    parser.add_argument('--base', type=str, default=None,
                        help='MLB 폴더 경로 (기본=스크립트 위치)')
    args = parser.parse_args()

    # 날짜 (EDT)
    if args.date:
        date_str = args.date
    else:
        edt = timezone(timedelta(hours=-4))
        date_str = datetime.now(edt).strftime('%Y-%m-%d')

    # 기본 경로
    base_dir = Path(args.base) if args.base else BASE_DIR

    print(f"=" * 60)
    print(f"MLB Syndicate ML Ensemble — {date_str}")
    print(f"BASE: {base_dir}")
    print(f"=" * 60)

    # 1. 학습 + 예측 데이터 단일 CSV에서 로드
    target = args.date  # 명시되면 그 날짜, 아니면 None → 마지막 미결 날짜
    df_wl, df_ou, df_predict, date_str = load_all_data(base_dir, target_date=target)

    if len(df_wl) < 50:
        print(f"⛔ HALT: 학습 데이터 {len(df_wl)}행 < 50행 최소 기준")
        sys.exit(1)

    # 피처 선택
    feature_cols = get_feature_cols(df_wl)

    # 피처 준비
    X_wl, imputer_wl = prepare_features(df_wl, feature_cols)
    y_wl = df_wl['y_wl']

    X_ou, imputer_ou = prepare_features(df_ou, feature_cols)
    y_ou = df_ou['y_ou']

    # 2. W/L 모델 학습
    wl_models, wl_scores, wl_scaler = train_wl_models(X_wl, y_wl, feature_cols)

    # 3. O/U 모델 학습
    ou_models, ou_scores, ou_scaler = train_ou_models(X_ou, y_ou, feature_cols)

    # 4. 예측 — df_predict의 각 행을 그대로 사용 (raw_stats.json 불필요)
    print(f"\n[4/6] 예측: {len(df_predict)}경기")
    results = predict_from_df(
        df_predict, feature_cols, imputer_wl,
        wl_models, wl_scaler, wl_scores,
        ou_models, ou_scaler, ou_scores
    )

    # 5. SHAP (DataFrame 기반)
    results = compute_shap_from_df(results, df_predict, feature_cols, imputer_wl,
                                   wl_models, ou_models)

    # 6. 저장
    n_features = len(feature_cols)
    out_path = save_output(
        results, date_str, base_dir,
        wl_scores, ou_scores, n_features,
        len(df_wl), len(df_ou), 100.0  # 매핑 100% (직접 컬럼 사용)
    )

    print(f"\n{'=' * 60}")
    print(f"✅ 완료. GitHub 자동 푸시 진행...")
    print(f"{'=' * 60}")

    # 자동 git add + commit + push
    auto_git_push(out_path, date_str, base_dir)


def auto_git_push(out_path, date_str: str, base_dir):
    """ml_predictions.json을 자동으로 git add + commit + push.
    Stale lock 자동 복구, push 실패 시 rebase 1회 시도 후 재푸시.
    """
    import subprocess
    import time as _time

    def run(cmd):
        return subprocess.run(cmd, cwd=str(base_dir), capture_output=True, text=True)

    # 0. git repo 여부 확인
    r = run(['git', 'rev-parse', '--is-inside-work-tree'])
    if r.returncode != 0:
        print(f"  [Git] Not a git repo — skipping push")
        return

    # 1. Stale lock 자동 제거 (10초 이상이면 즉시, 그 미만은 2초 대기 후 제거)
    lock_path = base_dir / '.git' / 'index.lock'
    if lock_path.exists():
        try:
            age = _time.time() - lock_path.stat().st_mtime
            if age > 10:
                lock_path.unlink()
                print(f"  [Git] Removed stale index.lock (age {age:.0f}s)")
            else:
                _time.sleep(2)
                if lock_path.exists():
                    lock_path.unlink()
                    print(f"  [Git] Removed lock after 2s wait")
        except OSError as e:
            print(f"  [Git] Could not remove lock: {e}")

    # 2. Stage the file
    rel_path = str(out_path.relative_to(base_dir)).replace('\\', '/')
    r = run(['git', 'add', '--', rel_path])
    if r.returncode != 0:
        print(f"  [Git] Stage failed: {r.stderr.strip()}")
        return

    # 3. 변경 사항 있는지 확인
    r = run(['git', 'diff', '--cached', '--quiet'])
    if r.returncode == 0:
        print(f"  [Git] No changes to push")
        return

    # 4. Commit
    msg = f"ml_predictions {date_str}"
    r = run(['git', 'commit', '-m', msg])
    if r.returncode != 0:
        print(f"  [Git] Commit failed: {r.stderr.strip()}")
        return
    print(f"  [Git] Committed: {msg}")

    # 5. Push to main only (no master fallback — prevents divergent branches)
    def push_attempt():
        r = run(['git', 'push', '-u', 'origin', 'HEAD:main'])
        return (r.returncode == 0, r.stderr.strip())

    ok, info = push_attempt()
    if ok:
        print(f"  [Git] Pushed to origin/main: {msg}")
        return

    # 푸시 실패 — non-fast-forward일 가능성. rebase 후 재시도
    print(f"  [Git] Push rejected, trying rebase+push...")
    rebase = run(['git', 'pull', 'origin', 'main', '--rebase', '--autostash'])
    if rebase.returncode != 0:
        print(f"  [Git] Rebase failed: {rebase.stderr.strip()}")
        return
    ok, info = push_attempt()
    if ok:
        print(f"  [Git] Pushed (after rebase) to origin/{info}: {msg}")
    else:
        print(f"  [Git] Push failed even after rebase: {info}")


if __name__ == '__main__':
    main()
