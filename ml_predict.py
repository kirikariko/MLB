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

# raw_stats.json → mlb_two.csv 컬럼 매핑
HOME_MAP = {
    'swp': 'SWP_HOMETEAM',
    'saps': 'SAPS_HOMETEAM',
    'sapa': 'SAPA_HOMETEAM',
    'win_rate': 'Home_Win_Rate',
    'saps_home_only': 'SAPS_HOME_ONLY_HOMETEAM',
    'sapa_home_only': 'SAPA_HOME_ONLY_HOMETEAM',
    'l5g_wp': 'L5G_WP_HOMETEAM',
    'l5g_avg_runs': 'L5G_avg_runs_HOME',
    'l5g_avg_runs_conceded': 'L5G_avg_runs_conceded_HOME',
    'h2h_wp': 'R_H2H_WP_HOMETEAM',
    'era_pitcher': 'ERA_HOMETEAM_PITCHER',
    'ba_total': 'AVG_BA_HOME_Total',
    'ba_vs_lhp': 'AVG_BA_HOME_VS_LHP',
    'ba_vs_rhp': 'AVG_BA_HOME_VS_RHP',
    'ba_homeonly': 'AVG_BA_HOME_HOMEONLY',
    'era_pitcher_homeonly': 'ERA_HOME_PITCHER_HOMEONLY',
    'ba_day': 'AVG_BA_HOME_DAY',
    'era_pitcher_day': 'ERA_HOME_PITCHER_DAY',
    'ba_night': 'AVG_BA_HOME_NIGHT',
    'era_pitcher_night': 'ERA_HOME_PITCHER_NIGHT',
    'obp': 'OBP_HOME',
    'era_team': 'ERA_HOMETEAM',
    'pm': 'Home_Team_PM',
    'bm': 'Home_Team_BM',
    'woba': 'HOMETEAM_wOBA',
    'siera': 'HOMETEAM_SIERA',
    'bp_era_30d': 'BP_ERA_30d_HOME',
    'bp_irs_30d': 'BP_IRS_30d_HOME',
    'bp_ip_3d': 'BP_IP_3d_HOME',
    'bullpen_workload': 'HOMETEAM_BULLPEN_WORKLOAD',
    'fpct': 'HOMETEAM_FPct',
    'errors': 'HOMETEAM_E',
    'bvp_ab': 'BVP_AB_HOME',
    'bvp_avg': 'BVP_AVG_HOME',
    'bvp_obp': 'BVP_OBP_HOME',
    'bvp_conf': 'BVP_CONFIDENCE_HOME',
    'road_streak': 'HOME_ROAD_STREAK',
    'travel_mi': 'HOME_TRAVEL_MI',
    'rest_days': 'HOME_REST_DAYS',
}

AWAY_MAP = {
    'swp': 'SWP_AWAYTEAM',
    'saps': 'SAPS_AWAYTEAM',
    'sapa': 'SAPA_AWAYTEAM',
    'win_rate_away': 'AwayTEAM_Win_Rate_ONLYAWAY',
    'saps_away_only': 'SAPS_AWAY_ONLY_AWAYTEAM',
    'sapa_away_only': 'SAPA_AWAY_ONLY_AWAYTEAM',
    'l5g_wp': 'L5G_WP_AWAYTEAM',
    'l5g_avg_runs': 'L5G_avg_runs_AWAY',
    'l5g_avg_runs_conceded': 'L5G_avg_runs_conceded_AWAY',
    'h2h_wp': 'R_H2H_WP_AWAYTEAM',
    'era_pitcher': 'ERA_AWAYTEAM_PITCHER',
    'ba_total': 'AVG_BA_AWAY_Total',
    'ba_vs_lhp': 'AVG_BA_AWAY_VS_LHP',
    'ba_vs_rhp': 'AVG_BA_AWAY_VS_RHP',
    'ba_awayonly': 'AVG_BA_AWAY_AWAYONLY',
    'era_pitcher_awayonly': 'ERA_AWAY_PITCHER_AWAYONLY',
    'ba_day': 'AVG_BA_AWAY_DAY',
    'era_pitcher_day': 'ERA_AWAY_PITCHER_DAY',
    'ba_night': 'AVG_BA_AWAY_NIGHT',
    'era_pitcher_night': 'ERA_AWAY_PITCHER_NIGHT',
    'obp': 'OBP_AWAY',
    'era_team': 'ERA_AWAYTEAM',
    'pm': 'AWAYTEAM_PM',
    'bm': 'AWAYTEAM_BM',
    'woba': 'AWAYTEAM_wOBA',
    'siera': 'AWAYTEAM_SIERA',
    'bp_era_30d': 'BP_ERA_30d_AWAY',
    'bp_irs_30d': 'BP_IRS_30d_AWAY',
    'bp_ip_3d': 'BP_IP_3d_AWAY',
    'bullpen_workload': 'AWAYTEAM_BULLPEN_WORKLOAD',
    'fpct': 'AWAYTEAM_FPct',
    'errors': 'AWAYTEAM_E',
    'bvp_ab': 'BVP_AB_AWAY',
    'bvp_avg': 'BVP_AVG_AWAY',
    'bvp_obp': 'BVP_OBP_AWAY',
    'bvp_conf': 'BVP_CONFIDENCE_AWAY',
    'road_streak': 'AWAY_ROAD_STREAK',
    'travel_mi': 'AWAY_TRAVEL_MI',
    'rest_days': 'AWAY_REST_DAYS',
}

SP_IP_MAP = {
    'home_sp_ip': 'HOME_SP_IP',
    'away_sp_ip': 'AWAY_SP_IP',
}

ENV_MAP = {
    'avg_temp_c': 'Avg_Temp_C',
    'precipitation_mm': 'Precipitation_mm',
    'max_wind_kph': 'Max_Wind_kph',
    'wind_dir_deg': 'WIND_DIR_DEG',
}

META_MAP = {
    'series_game_num': 'Series_Game_Num',
    'series_home_wins': 'Series_Home_Wins',
    'series_away_wins': 'Series_Away_Wins',
    'uo_line': 'UO_LINE',
    'park_factor': 'PARK_FACTOR',
    'ump_rpg': 'UMP_RPG',
}


# ============================================================
# DATA LOADING
# ============================================================
def load_training_data(base_dir: Path):
    """mlb_two.csv에서 학습 데이터 로드"""
    csv_path = base_dir / 'data' / 'mlb_two.csv'
    print(f"[1/6] 학습 데이터 로드: {csv_path}")

    df = pd.read_csv(csv_path)
    print(f"  전체 행: {len(df)}, 컬럼: {len(df.columns)}")

    # W/L 타겟: "1"=홈승, "0"=원정승
    valid_wl = df['Winning_Team'].isin(['1', '0'])
    df_wl = df[valid_wl].copy()
    df_wl['y_wl'] = df_wl['Winning_Team'].astype(int)
    print(f"  W/L 유효 행: {len(df_wl)}")

    # O/U 타겟: "1"=오버, "0"=언더
    valid_ou = df['UO_RESULT'].isin(['1', '0'])
    df_ou = df[valid_ou].copy()
    df_ou['y_ou'] = df_ou['UO_RESULT'].astype(int)
    print(f"  O/U 유효 행: {len(df_ou)}")

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
# PREDICTION — raw_stats.json → feature vector
# ============================================================
def game_to_feature_row(game: dict, feature_cols: list) -> dict:
    """raw_stats.json의 1경기 → mlb_two.csv 컬럼명 dict 변환"""
    row = {}

    # Home 매핑
    home = game.get('home', {})
    for src_key, dst_col in HOME_MAP.items():
        val = home.get(src_key)
        if val is not None and dst_col in feature_cols:
            row[dst_col] = _to_float(val)

    # Away 매핑
    away = game.get('away', {})
    for src_key, dst_col in AWAY_MAP.items():
        val = away.get(src_key)
        if val is not None and dst_col in feature_cols:
            row[dst_col] = _to_float(val)

    # SP IP 매핑
    sp_ip = game.get('sp_ip', {})
    for src_key, dst_col in SP_IP_MAP.items():
        val = sp_ip.get(src_key)
        if val is not None and dst_col in feature_cols:
            row[dst_col] = _to_float(val)

    # Environment 매핑
    env = game.get('environment_basic', {})
    if isinstance(env, dict):
        weather = env.get('weather', env)
        if isinstance(weather, dict):
            for src_key, dst_col in ENV_MAP.items():
                val = weather.get(src_key)
                if val is not None and dst_col in feature_cols:
                    row[dst_col] = _to_float(val)
        # Park factor
        pf = env.get('park_factor')
        if isinstance(pf, dict):
            rf = pf.get('runs_factor')
            if rf is not None and 'PARK_FACTOR' in feature_cols:
                # park_factor in raw_stats is scale 80-120, csv uses same
                row['PARK_FACTOR'] = _to_float(rf)
        elif pf is not None and 'PARK_FACTOR' in feature_cols:
            row['PARK_FACTOR'] = _to_float(pf)

        # Umpire
        ump = env.get('umpire', {})
        if isinstance(ump, dict):
            rpg = ump.get('rpg') or ump.get('ump_rpg')
            if rpg is not None and 'UMP_RPG' in feature_cols:
                row['UMP_RPG'] = _to_float(rpg)

    # Series 매핑
    series = game.get('series', {})
    if isinstance(series, dict):
        sgn = series.get('game_num') or series.get('series_game_num')
        if sgn is not None and 'Series_Game_Num' in feature_cols:
            row['Series_Game_Num'] = _to_float(sgn)
        shw = series.get('home_wins') or series.get('series_home_wins')
        if shw is not None and 'Series_Home_Wins' in feature_cols:
            row['Series_Home_Wins'] = _to_float(shw)
        saw = series.get('away_wins') or series.get('series_away_wins')
        if saw is not None and 'Series_Away_Wins' in feature_cols:
            row['Series_Away_Wins'] = _to_float(saw)

    # UO_LINE from odds or environment
    uo = game.get('uo_line')
    if uo is not None and 'UO_LINE' in feature_cols:
        row['UO_LINE'] = _to_float(uo)

    return row


def _to_float(val):
    """안전한 float 변환"""
    if val is None:
        return np.nan
    try:
        return float(val)
    except (ValueError, TypeError):
        return np.nan


def predict_games(games: list, feature_cols: list, imputer,
                  wl_models: dict, wl_scaler, wl_scores: dict,
                  ou_models: dict, ou_scaler, ou_scores: dict):
    """오늘 경기 예측"""
    print(f"\n[4/6] 경기 예측 ({len(games)}경기)")

    results = []

    for game in games:
        game_id = game['game_id']
        row_dict = game_to_feature_row(game, feature_cols)

        # 매핑률 계산
        mapped = sum(1 for c in feature_cols if c in row_dict and not np.isnan(row_dict.get(c, np.nan)))
        mapping_pct = mapped / len(feature_cols) * 100

        # DataFrame으로 변환 + imputation
        row_df = pd.DataFrame([{c: row_dict.get(c, np.nan) for c in feature_cols}])
        row_imputed = pd.DataFrame(
            imputer.transform(row_df),
            columns=feature_cols
        )

        # W/L 예측
        wl_preds = {}
        row_scaled = wl_scaler.transform(row_imputed)

        if 'lr' in wl_models:
            p = wl_models['lr'].predict_proba(row_scaled)[0][1]
            wl_preds['lr'] = float(p)
        if 'xgb' in wl_models:
            p = wl_models['xgb'].predict_proba(row_imputed)[0][1]
            wl_preds['xgb'] = float(p)
        if 'lgb' in wl_models:
            p = wl_models['lgb'].predict_proba(row_imputed)[0][1]
            wl_preds['lgb'] = float(p)

        # 가중 앙상블 (CV accuracy 기반 가중치)
        wl_ensemble = _weighted_avg(wl_preds, wl_scores)

        # O/U 예측
        ou_preds = {}
        row_ou_scaled = ou_scaler.transform(row_imputed)

        if 'lr' in ou_models:
            p = ou_models['lr'].predict_proba(row_ou_scaled)[0][1]
            ou_preds['lr'] = float(p)
        if 'xgb' in ou_models:
            p = ou_models['xgb'].predict_proba(row_imputed)[0][1]
            ou_preds['xgb'] = float(p)
        if 'lgb' in ou_models:
            p = ou_models['lgb'].predict_proba(row_imputed)[0][1]
            ou_preds['lgb'] = float(p)

        ou_ensemble = _weighted_avg(ou_preds, ou_scores)

        result = {
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
        }

        results.append(result)
        print(f"  {game_id}: WL={wl_ensemble:.3f} (LR={wl_preds.get('lr','N/A'):.3f} "
              f"XGB={wl_preds.get('xgb','N/A'):.3f} LGB={wl_preds.get('lgb','N/A'):.3f}) "
              f"mapping={mapping_pct:.0f}%")

    return results


def _weighted_avg(preds: dict, scores: dict) -> float:
    """CV accuracy 기반 가중 평균"""
    total_w = 0
    total_p = 0
    for name, pred in preds.items():
        w = scores.get(name, 0.5)
        total_w += w
        total_p += pred * w
    return total_p / total_w if total_w > 0 else 0.5


# ============================================================
# SHAP EXPLANATION
# ============================================================
def compute_shap(results: list, games: list, feature_cols: list,
                 imputer, wl_models: dict, ou_models: dict):
    """SHAP 피처 중요도 계산"""
    if not HAS_SHAP:
        print("\n[5/6] SHAP 스킵 (shap 미설치)")
        return results

    print(f"\n[5/6] SHAP 계산")

    # XGBoost 모델 우선, 없으면 LightGBM
    wl_model = wl_models.get('xgb') or wl_models.get('lgb')
    ou_model = ou_models.get('xgb') or ou_models.get('lgb')

    if wl_model is None:
        print("  SHAP 스킵 (tree 모델 없음)")
        return results

    try:
        wl_explainer = shap.TreeExplainer(wl_model)
        ou_explainer = shap.TreeExplainer(ou_model) if ou_model else None

        for i, game in enumerate(games):
            row_dict = game_to_feature_row(game, feature_cols)
            row_df = pd.DataFrame([{c: row_dict.get(c, np.nan) for c in feature_cols}])
            row_imputed = pd.DataFrame(
                imputer.transform(row_df),
                columns=feature_cols
            )

            # W/L SHAP
            shap_vals = wl_explainer.shap_values(row_imputed)
            if isinstance(shap_vals, list):
                sv = shap_vals[1][0]  # class 1 (home win)
            else:
                sv = shap_vals[0]

            # Top 10 피처
            top_idx = np.argsort(np.abs(sv))[-10:][::-1]
            shap_features = []
            for idx in top_idx:
                shap_features.append({
                    'feature': feature_cols[idx],
                    'shap_value': round(float(sv[idx]), 4),
                    'feature_value': round(float(row_imputed.iloc[0, idx]), 4)
                })

            results[i]['shap_features'] = shap_features

            # Feature importance (전체)
            fi = dict(zip(feature_cols, [round(float(x), 4) for x in np.abs(sv)]))
            fi_sorted = dict(sorted(fi.items(), key=lambda x: x[1], reverse=True)[:20])
            results[i]['feature_importance'] = fi_sorted

        print(f"  SHAP 완료 ({len(games)}경기)")
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

    # 1. 학습 데이터 로드
    df_wl, df_ou = load_training_data(base_dir)

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

    # 4. 오늘 경기 로드 + 예측
    raw_path = base_dir / 'pipeline' / date_str / 'dept1' / 'raw_stats.json'
    if not raw_path.exists():
        print(f"⛔ HALT: {raw_path} 없음")
        sys.exit(1)

    with open(raw_path, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    games = raw_data.get('games', [])
    print(f"  오늘 경기: {len(games)}개")

    # O/U 라인 보충 (odds.json에서)
    odds_path = base_dir / 'pipeline' / date_str / 'dept1' / 'odds.json'
    if odds_path.exists():
        with open(odds_path, 'r', encoding='utf-8') as f:
            odds_data = json.load(f)
        odds_map = {g['game_id']: g for g in odds_data.get('games', [])}
        for game in games:
            gid = game['game_id']
            if gid in odds_map and 'uo_line' not in game:
                totals = odds_map[gid].get('totals', {})
                if 'over_line' in totals:
                    game['uo_line'] = totals['over_line']

    # 예측 (W/L imputer 사용 — 동일 피처셋)
    results = predict_games(
        games, feature_cols, imputer_wl,
        wl_models, wl_scaler, wl_scores,
        ou_models, ou_scaler, ou_scores
    )

    # 5. SHAP
    results = compute_shap(results, games, feature_cols, imputer_wl, wl_models, ou_models)

    # 6. 저장
    avg_mapping = np.mean([r['mapping_pct'] for r in results])
    out_path = save_output(
        results, date_str, base_dir,
        wl_scores, ou_scores, len(feature_cols),
        len(df_wl), len(df_ou), avg_mapping
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

    # 5. Push — 실패 시 rebase 1회 시도 후 재푸시
    def push_attempt():
        for branch in ('main', 'master'):
            r = run(['git', 'push', '-u', 'origin', f'HEAD:{branch}'])
            if r.returncode == 0:
                return True, branch
        return False, r.stderr.strip()

    ok, info = push_attempt()
    if ok:
        print(f"  [Git] Pushed to origin/{info}: {msg}")
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
