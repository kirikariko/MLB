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
    'Series_Game_Num',
    # SP Statcast 투수실력 지표 (2026-08-06 추가). 과거 행 NaN → 초기엔 95% 필터에 걸리므로
    # 보호 피처로 유지해 누적. 데이터 쌓이면 순수 독립 신호로 작동 (배당 오염 없음).
    'HOME_SP_xwOBA', 'AWAY_SP_xwOBA',
    'HOME_SP_HardHit', 'AWAY_SP_HardHit',
    'HOME_SP_Barrel', 'AWAY_SP_Barrel',
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
                cwd=str(repo_root), capture_output=True, text=True,
                encoding='utf-8', errors='replace', timeout=30,
            )
            if r.returncode != 0:
                last_err = f"git fetch failed: {(r.stderr or '').strip()}"
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


# ============================================================
# 확정 선발 동기화 (2026-08-17 박음 — I-8 사고)
# ============================================================
# 사고 내용:
#   mlb_two.csv(=MLB_KING.csv 파생)는 KING 시점의 "예고 선발" 성적을 담는다.
#   dept1은 등판 확정 후 RotoWire로 실제 선발을 확인해 sp_profiles.json에 기록하며,
#   KING과 0.5 이상 차이 나면 era_source="RotoWire(KING stale >0.5 diff)"로 표시한다.
#   Poisson(dept2)은 sp_profiles를 읽어 신선한 값을 쓰지만,
#   ML(ml_predict.py)은 mlb_two.csv를 직접 읽어 낡은 값을 그대로 학습/예측에 썼다.
#   → 같은 부서 두 엔진이 서로 다른 투수를 보고 예측했고,
#     그 인위적 이견을 dept3가 "모델 vs 시장 이견"으로 오인해 확률을 왜곡 조정했다.
#   실사고: 2026-08-16 MIN_PHI — Poisson ERA 5.25(Dean Kremer 확정) vs ML ERA 6.50(취소된 예고선발).
#           Poisson 홈 63.9% vs ML 홈 40.5% (23.4%p 가짜 이견) → 2폴드 오발행 → 패.
#
# 조치: 예측 대상 행의 선발 관련 피처를 dept1 sp_profiles.json 확정값으로 덮어쓴다.
#   - 확정값이 있는 피처만 덮어쓴다.
#   - 확정값이 없는 파생 피처(HOMEONLY/DAY/NIGHT 분할 ERA 등)는 "다른 투수의 값"이므로
#     NaN으로 비운다. median imputation이 처리한다. ⛔ 추정값 채워넣기(날조) 금지.
#   - 학습 데이터는 건드리지 않는다 (과거 행은 이미 실제 등판 결과가 반영된 확정 데이터).

# sp_profiles 필드 → mlb_two.csv 컬럼 매핑 (확정값 존재)
SP_OVERRIDE_MAP = {
    'home': {
        'era':            'ERA_HOMETEAM_PITCHER',
        'ip':             'HOME_SP_IP',
        'xfip':           'HOME_SP_xFIP',
        'xwoba':          'HOME_SP_xwOBA',
        'hard_hit':       'HOME_SP_HardHit',
        'barrel':         'HOME_SP_Barrel',
        'fb_velo':        'HOME_SP_FB_Velo',
        'fb_velo_trend':  'HOME_SP_FB_Velo_Trend',
    },
    'away': {
        'era':            'ERA_AWAYTEAM_PITCHER',
        'ip':             'AWAY_SP_IP',
        'xfip':           'AWAY_SP_xFIP',
        'xwoba':          'AWAY_SP_xwOBA',
        'hard_hit':       'AWAY_SP_HardHit',
        'barrel':         'AWAY_SP_Barrel',
        'fb_velo':        'AWAY_SP_FB_Velo',
        'fb_velo_trend':  'AWAY_SP_FB_Velo_Trend',
    },
}

# 확정값이 없는 파생 피처 → 선발이 바뀌면 무효화 (NaN)
SP_INVALIDATE = {
    'home': ['ERA_HOME_PITCHER_HOMEONLY', 'ERA_HOME_PITCHER_DAY', 'ERA_HOME_PITCHER_NIGHT'],
    'away': ['ERA_AWAY_PITCHER_AWAYONLY', 'ERA_AWAY_PITCHER_DAY', 'ERA_AWAY_PITCHER_NIGHT'],
}

# ERA가 이만큼 이상 차이나면 "선발이 바뀐 것"으로 간주 (dept1의 stale 판정과 동일 기준)
SP_STALE_ERA_DIFF = 0.5

# dept2 Poisson이 쓰는 ERA 축소(shrinkage) 상수.
# 2026-08-17 역산 검증: base_probabilities.json의 era_shrinkage 102쌍 전부에서
#   shrunk = (IP*raw + K*PRIOR) / (IP + K),  K=70.0, PRIOR=4.15  — 오차 0.02 이내로 일치.
# ⛔ ML 피처는 raw ERA를 그대로 쓴다. 학습 행(과거)이 raw이므로 예측 행만 축소하면
#    학습/예측 분포가 어긋나는 새 버그가 된다. 축소값은 참고 지표로만 기록한다.
SP_SHRINK_K = 70.0
SP_SHRINK_PRIOR = 4.15
# 이닝이 이보다 적으면 ERA가 극단값(0.00, 10.13 등)이 되어 신뢰할 수 없다 → 플래그만 남긴다.
SP_LOW_IP_THRESHOLD = 20.0

# 물리적으로 불가능한 값 방어 (2026-08-17 발견).
# 실제 사례: 2026-08-15 TOR_NYY 홈 선발 sp_profiles era=0.00 / ip=61.0.
# MLB 역사상 규정이닝급 0.00 ERA는 존재하지 않는다 → 원천 데이터 결손이다.
# 이런 값으로 덮어쓰면 낡은 값보다 더 나빠지므로 덮어쓰지 않고 플래그만 남긴다.
# ⛔ 값을 임의 추정해 채우지 않는다 (날조 금지). 판단은 dept3/dept4에 넘긴다.
SP_IMPLAUSIBLE_IP = 30.0


def is_implausible_era(era, ip):
    """ERA 0.00인데 이닝이 충분히 많으면 원천 데이터 결손으로 본다."""
    try:
        era, ip = float(era), float(ip)
    except (TypeError, ValueError):
        return False
    return era <= 0.0 and ip >= SP_IMPLAUSIBLE_IP


def shrunk_era(raw, ip):
    """dept2와 동일한 ERA 축소값 (참고용)."""
    try:
        raw, ip = float(raw), float(ip)
    except (TypeError, ValueError):
        return None
    return round((ip * raw + SP_SHRINK_K * SP_SHRINK_PRIOR) / (ip + SP_SHRINK_K), 4)


def _iter_sp_games(obj):
    """sp_profiles.json 구조가 바뀌어도 game_id+home_sp를 가진 dict를 찾아낸다."""
    if isinstance(obj, dict):
        if 'game_id' in obj and ('home_sp' in obj or 'away_sp' in obj):
            yield obj
        for v in obj.values():
            yield from _iter_sp_games(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _iter_sp_games(v)


def apply_confirmed_sp(df_predict, date_str: str, base_dir: Path):
    """예측 행의 선발 피처를 dept1 확정 선발값으로 동기화.

    Returns: (df_predict, report_dict)
    """
    print(f"\n[1.5/6] 확정 선발 동기화 (dept1 sp_profiles)")
    report = {
        'applied': False,
        'source': None,
        'games_checked': 0,
        'games_overridden': 0,
        'overrides': [],
        'note': None,
    }

    sp_path = base_dir / 'pipeline' / date_str / 'dept1' / 'sp_profiles.json'
    if not sp_path.exists():
        msg = f"sp_profiles.json 없음 ({sp_path}) — KING 값 그대로 사용"
        print(f"  ⚠ {msg}")
        report['note'] = msg
        return df_predict, report

    try:
        with open(sp_path, 'r', encoding='utf-8') as f:
            sp_raw = json.load(f)
    except Exception as e:
        msg = f"sp_profiles.json 파싱 실패: {e} — KING 값 그대로 사용"
        print(f"  ⚠ {msg}")
        report['note'] = msg
        return df_predict, report

    sp_by_gid = {g['game_id']: g for g in _iter_sp_games(sp_raw)}
    report['applied'] = True
    report['source'] = str(sp_path.relative_to(base_dir)) if str(sp_path).startswith(str(base_dir)) else str(sp_path)

    if 'GAME_ID' not in df_predict.columns:
        msg = "df_predict에 GAME_ID 컬럼 없음 — 동기화 불가"
        print(f"  ⛔ {msg}")
        report['applied'] = False
        report['note'] = msg
        return df_predict, report

    for idx, row in df_predict.iterrows():
        gid = str(row['GAME_ID'])
        g = sp_by_gid.get(gid)
        if not g:
            continue
        report['games_checked'] += 1
        changed_fields = []
        stale_sides = []
        low_ip_sides = []
        suspect_sides = []
        shrunk_ref = {}

        for side in ('home', 'away'):
            prof = g.get(f'{side}_sp') or {}
            if not prof:
                continue

            # 선발 교체 여부 판정: dept1의 era_source 표시 우선, 없으면 ERA 차이로 판정
            era_new = prof.get('era')
            era_col = SP_OVERRIDE_MAP[side]['era']
            era_old = pd.to_numeric(pd.Series([row.get(era_col)]), errors='coerce').iloc[0]
            src = str(prof.get('era_source') or '')
            starter_changed = ('stale' in src.lower()) or (
                era_new is not None and pd.notna(era_old)
                and abs(float(era_new) - float(era_old)) >= SP_STALE_ERA_DIFF
            )

            # 표본 부족 경고 — 피처는 건드리지 않고 플래그만 (⛔ 값 조작 금지)
            ip_new = prof.get('ip')
            try:
                if ip_new is not None and float(ip_new) < SP_LOW_IP_THRESHOLD:
                    low_ip_sides.append(side)
            except (TypeError, ValueError):
                pass
            sh = shrunk_era(era_new, ip_new)
            if sh is not None:
                shrunk_ref[side] = {'raw': era_new, 'ip': ip_new, 'shrunk_dept2_equiv': sh}

            # 원천 데이터 결손 방어 — 덮어쓰지 않고 그 side 전체를 건너뛴다
            if is_implausible_era(era_new, prof.get('ip')):
                suspect_sides.append(side)
                print(f"  ⚠ {gid} {side}_sp ERA={era_new} IP={prof.get('ip')} — 물리적 불가능값, 동기화 건너뜀")
                continue

            # 확정값이 있는 피처는 항상 동기화 (선발 교체 여부와 무관 — 최신본이 진실)
            for fld, col in SP_OVERRIDE_MAP[side].items():
                val = prof.get(fld)
                if val is None or col not in df_predict.columns:
                    continue
                old = pd.to_numeric(pd.Series([row.get(col)]), errors='coerce').iloc[0]
                try:
                    new = float(val)
                except (TypeError, ValueError):
                    continue
                if pd.isna(old) or abs(old - new) > 1e-9:
                    df_predict.at[idx, col] = new
                    changed_fields.append({
                        'col': col,
                        'from': None if pd.isna(old) else round(float(old), 4),
                        'to': round(new, 4),
                    })

            # 선발이 바뀐 경우: 확정값 없는 파생 피처는 "다른 투수의 값"이므로 무효화
            if starter_changed:
                stale_sides.append(side)
                for col in SP_INVALIDATE[side]:
                    if col in df_predict.columns and pd.notna(
                        pd.to_numeric(pd.Series([row.get(col)]), errors='coerce').iloc[0]
                    ):
                        df_predict.at[idx, col] = float('nan')
                        changed_fields.append({'col': col, 'from': 'stale', 'to': None})

        if changed_fields or low_ip_sides or suspect_sides:
            if changed_fields:
                report['games_overridden'] += 1
            report['overrides'].append({
                'game_id': gid,
                'starter_changed_sides': stale_sides,
                'sp_stats_stale': bool(stale_sides),
                'sp_low_ip_sides': low_ip_sides,
                'sp_low_ip': bool(low_ip_sides),
                'sp_era_suspect_sides': suspect_sides,
                'sp_era_suspect': bool(suspect_sides),
                'era_shrink_reference': shrunk_ref,
                'home_confirmed_starter': (g.get('home_sp') or {}).get('confirmed_starter'),
                'away_confirmed_starter': (g.get('away_sp') or {}).get('confirmed_starter'),
                'fields': changed_fields,
            })
            tag = ''
            if stale_sides:
                tag += ' ⚠선발교체'
            if low_ip_sides:
                tag += f" ⚠표본부족(IP<{SP_LOW_IP_THRESHOLD:.0f}): {','.join(low_ip_sides)}"
            if suspect_sides:
                tag += f" ⛔원천데이터결손: {','.join(suspect_sides)}"
            print(f"  {gid}: {len(changed_fields)}개 피처 동기화{tag}")

    print(f"  대상 {report['games_checked']}경기 중 {report['games_overridden']}경기 동기화")
    if report['games_overridden'] == 0:
        print(f"  (KING 값과 dept1 확정값이 일치 — 조정 없음)")
    return df_predict, report


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
    # keep_empty_features=True: 전부 NaN인 보호 피처(신규 Statcast 등, 아직 누적 전)를
    # 버리지 않고 유지해 shape 불일치 크래시 방지. 실값 쌓이면 median 대치 정상 작동. (2026-08-12)
    imputer = SimpleImputer(strategy='median', keep_empty_features=True)
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
# EDGE vs MARKET (배당은 피처가 아니라 '판단 기준선' — 오염 방지 설계, 2026-08-06)
# ============================================================
def compute_edge_from_odds(results: list, date_str: str, base_dir: Path):
    """모델 확률(p_model)과 시장 vig-제거 확률(p_market)의 엣지 계산.

    ⛔ 배당은 학습 피처로 절대 넣지 않는다 (모델이 배당만 베끼는 오염 방지).
       odds.json은 '판단 단계'에서 기준선으로만 사용한다.
    edge = p_model - p_market  (양수 = 모델이 시장보다 그 쪽을 높게 봄 = 밸류 후보)
    odds.json 없거나 게임 미매칭 시 조용히 skip (market_available=False, 크래시 없음).
    """
    odds_path = Path(base_dir) / 'pipeline' / date_str / 'dept1' / 'odds.json'
    if not odds_path.exists():
        print(f"\n[Edge] odds.json 없음 ({odds_path}) — 엣지 skip (모델 예측은 정상)")
        for r in results:
            r['market_available'] = False
        return results

    try:
        raw = odds_path.read_bytes().replace(b'\x00', b'').decode('utf-8', 'ignore')
        odds = json.loads(raw)
    except Exception as e:
        print(f"\n[Edge] odds.json 파싱 실패: {e} — skip")
        for r in results:
            r['market_available'] = False
        return results

    omap = {g.get('game_id'): g for g in odds.get('games', [])}
    print(f"\n[Edge] 시장 대비 엣지 계산 ({len(omap)}경기 배당 로드)")

    n_matched = 0
    for r in results:
        g = omap.get(r.get('game_id'))
        if not g:
            r['market_available'] = False
            continue
        n_matched += 1
        r['market_available'] = True
        ml = g.get('moneyline') or {}
        r['home_ml'] = ml.get('home')
        r['away_ml'] = ml.get('away')

        # W/L 엣지 (true_prob = vig 제거 시장확률)
        tp = g.get('true_prob') or {}
        mh, ma = tp.get('home'), tp.get('away')
        if mh is not None and ma is not None:
            r['market_home_prob'] = round(float(mh), 4)
            r['market_away_prob'] = round(float(ma), 4)
            eh = round(r['ml_home_win_prob'] - float(mh), 4)
            ea = round(r['ml_away_win_prob'] - float(ma), 4)
            r['wl_edge_home'], r['wl_edge_away'] = eh, ea
            r['wl_value_side'], r['wl_value_edge'] = ('home', eh) if eh >= ea else ('away', ea)

        # O/U 엣지 (total_field.over_true/under_true = vig 제거 시장확률)
        tf = g.get('total_field') or {}
        ot, ut = tf.get('over_true'), tf.get('under_true')
        if ot is not None and ut is not None:
            r['ou_market_over_prob'] = round(float(ot), 4)
            r['ou_market_under_prob'] = round(float(ut), 4)
            eo = round(r['ml_over_prob'] - float(ot), 4)
            eu = round(r['ml_under_prob'] - float(ut), 4)
            r['ou_edge_over'], r['ou_edge_under'] = eo, eu
            r['ou_value_side'], r['ou_value_edge'] = ('over', eo) if eo >= eu else ('under', eu)

        print(f"  {r['game_id']}: WL엣지 {r.get('wl_value_side','-')} {r.get('wl_value_edge',0):+.3f}"
              f" | OU엣지 {r.get('ou_value_side','-')} {r.get('ou_value_edge',0):+.3f}")

    print(f"  매칭: {n_matched}/{len(results)}경기 (미매칭은 market_available=False)")
    return results


# ============================================================
# OUTPUT
# ============================================================
def save_output(results: list, date_str: str, base_dir: Path,
                wl_scores: dict, ou_scores: dict, n_features: int,
                n_train_wl: int, n_train_ou: int, avg_mapping_pct: float,
                sp_sync: dict = None):
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
        'sp_source': 'dept1/sp_profiles.json (확정 선발 동기화)',
        'sp_sync': sp_sync or {'applied': False, 'note': 'apply_confirmed_sp 미실행'},
        'games': results,
    }

    # 선발 교체가 감지된 경기는 게임 레코드에도 플래그를 남긴다 (dept3/dept4가 읽는다)
    if sp_sync and sp_sync.get('overrides'):
        stale_map = {o['game_id']: o for o in sp_sync['overrides']}
        for r in results:
            o = stale_map.get(r.get('game_id'))
            if not o:
                continue
            r['sp_synced'] = True
            r['sp_stats_stale'] = bool(o.get('sp_stats_stale'))
            r['sp_starter_changed_sides'] = o.get('starter_changed_sides') or []
            r['sp_low_ip'] = bool(o.get('sp_low_ip'))
            r['sp_low_ip_sides'] = o.get('sp_low_ip_sides') or []
            r['sp_era_suspect'] = bool(o.get('sp_era_suspect'))
            r['sp_era_suspect_sides'] = o.get('sp_era_suspect_sides') or []
            r['era_shrink_reference'] = o.get('era_shrink_reference') or {}
            r['confirmed_starters'] = {
                'home': o.get('home_confirmed_starter'),
                'away': o.get('away_confirmed_starter'),
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

    # 1.5 확정 선발 동기화 — Poisson(dept2)과 ML이 같은 투수를 보게 한다 (I-8, 2026-08-17)
    df_predict, sp_sync = apply_confirmed_sp(df_predict, date_str, base_dir)

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

    # 5.5 배당 대비 엣지 (배당은 피처가 아니라 판단 기준선 — 오염 방지)
    results = compute_edge_from_odds(results, date_str, base_dir)

    # 6. 저장
    n_features = len(feature_cols)
    out_path = save_output(
        results, date_str, base_dir,
        wl_scores, ou_scores, n_features,
        len(df_wl), len(df_ou), 100.0,  # 매핑 100% (직접 컬럼 사용)
        sp_sync=sp_sync
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
        # encoding='utf-8', errors='replace': 한글 Windows에서 git 출력이 UTF-8 한글을
        # 담을 때 기본 cp949 디코딩이 UnicodeDecodeError로 죽어 stderr=None이 되는 사고 방지 (2026-08-15)
        return subprocess.run(cmd, cwd=str(base_dir), capture_output=True,
                              text=True, encoding='utf-8', errors='replace')

    # 0. git repo 여부 확인
    r = run(['git', 'rev-parse', '--is-inside-work-tree'])
    if r.returncode != 0:
        print(f"  [Git] Not a git repo — skipping push")
        return

    # 1. Stale lock 자동 제거 (10초 이상이면 즉시, 그 미만은 2초 대기 후 제거)
    #    index.lock 뿐 아니라 HEAD.lock 도 정리 (중단된 git이 남기면 commit이 'cannot lock ref HEAD'로 막힘, 2026-08-15)
    for lock_name in ('index.lock', 'HEAD.lock'):
        lock_path = base_dir / '.git' / lock_name
        if lock_path.exists():
            try:
                age = _time.time() - lock_path.stat().st_mtime
                if age > 10:
                    lock_path.unlink()
                    print(f"  [Git] Removed stale {lock_name} (age {age:.0f}s)")
                else:
                    _time.sleep(2)
                    if lock_path.exists():
                        lock_path.unlink()
                        print(f"  [Git] Removed {lock_name} after 2s wait")
            except OSError as e:
                print(f"  [Git] Could not remove {lock_name}: {e}")

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
