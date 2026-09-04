#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""warehouse_build.py — MLB 경기·타석 데이터 창고 (2026-09-04 사장님 지시)

왜 만드나:
  "시장이 못 보는 우리만의 데이터"를 찾으려면 남이 조립한 열이 아니라 원재료(타석 단위)가 있어야 한다.
  지금은 2026 한 시즌 ~1,000경기라 가설마다 "표본 부족"에 막힌다. 11시즌이면 판정이 된다.

원천: MLB StatsAPI (statsapi.mlb.com) — 경기당 live feed 1회 호출.
  ⛔ Claude 샌드박스는 이 도메인이 403 이다. 사장님 PC 에서 `창고만들기.bat` 로 돌린다.
  ⛔ 배당은 수집하지 않는다 (9.0-BLIND 무관 자료).

산출 (data/warehouse/):
  games_{YYYY}.csv.gz     경기당 팀별 1줄 (MoneyPuck all_teams.csv 형태). 2줄/경기
  pa_{YYYY}.csv.gz        타석당 1줄 — 이닝·아웃·주자상황(menOnBase)·카운트·결과·득점
  pitching_{YYYY}.csv.gz  투수 등판당 1줄 — 선발여부·이닝·투구수 (연투·피로 계산용)
  _done_{YYYY}.json       처리 완료 gamePk 목록 (재실행 시 이어받기)
  _build_log.txt

원칙:
  - 없는 값은 빈칸. 추정으로 채우지 않는다.
  - 정규시즌(gameType R) + 상태 Final 만.
  - 재실행 안전 (멱등). 중단돼도 다시 누르면 이어서 받는다.

사용:
  python scripts/warehouse_build.py                     # 2015~올해
  python scripts/warehouse_build.py --seasons 2024-2026 --workers 6
"""
import os, sys, csv, gzip, json, time, argparse, threading
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(BASE, 'data', 'warehouse')
API = 'https://statsapi.mlb.com/api'
UA = {'User-Agent': 'mlb-warehouse/1.0 (research; contact min45652@gmail.com)'}
LOCK = threading.Lock()

GAME_COLS = ['season', 'date', 'gamePk', 'game_type', 'doubleheader', 'game_num', 'team', 'team_id', 'opp', 'opp_id',
             'home', 'team_score', 'opp_score', 'won', 'innings', 'day_night', 'start_time_utc', 'venue',
             'temp_f', 'wind', 'condition', 'hp_umpire',
             'sp_id', 'sp_name', 'sp_hand', 'sp_ip', 'sp_outs', 'sp_pitches', 'sp_er', 'sp_k', 'sp_bb', 'sp_h',
             'pitchers_used', 'bp_pitches', 'bp_er', 'bp_outs',
             'ab', 'r', 'h', 'doubles', 'triples', 'hr', 'rbi', 'bb', 'so', 'hbp', 'sb', 'cs', 'lob', 'gidp',
             'pa', 'risp_pa', 'risp_k', 'risp_bb', 'risp_hits', 'risp_runs', 'men_on_pa', 'empty_pa',
             'two_strike_pa', 'two_strike_k', 'runs_1st3', 'runs_last3',
             'catcher_id', 'catcher_name', 'lineup_ids', 'lineup_pos']
PA_COLS = ['season', 'date', 'gamePk', 'inning', 'half', 'bat_team', 'pit_team', 'batter_id', 'batter', 'bat_side',
           'pitcher_id', 'pitcher', 'pitch_hand', 'pitcher_is_sp', 'pitcher_pitch_no', 'outs_before', 'men_on',
           'away_score_before', 'home_score_before', 'balls', 'strikes', 'n_pitches', 'event', 'event_type',
           'is_k', 'is_bb', 'is_hit', 'is_hr', 'is_out', 'rbi', 'runs_scored', 'two_strike']
PIT_COLS = ['season', 'date', 'gamePk', 'team', 'pitcher_id', 'pitcher', 'hand', 'order', 'is_starter',
            'ip', 'outs', 'pitches', 'strikes', 'h', 'er', 'r', 'k', 'bb', 'hr', 'bf', 'decision']


def log(msg):
    line = '[%s] %s' % (datetime.now().strftime('%H:%M:%S'), msg)
    with LOCK:
        print(line, flush=True)
        with open(os.path.join(OUT, '_build_log.txt'), 'a', encoding='utf-8') as f:
            f.write(line + '\n')


def get_json(url, tries=4):
    for i in range(tries):
        try:
            with urlopen(Request(url, headers=UA), timeout=40) as r:
                return json.loads(r.read().decode('utf-8'))
        except (HTTPError, URLError, TimeoutError, OSError) as e:
            if i == tries - 1:
                raise
            time.sleep(2 * (i + 1))


def ip_to_outs(ip):
    """'5.2' → 17 (야구식 표기: .1=1아웃 .2=2아웃)"""
    if ip in (None, ''):
        return None
    try:
        s = str(ip)
        whole, _, frac = s.partition('.')
        return int(whole) * 3 + int(frac or 0)
    except Exception:
        return None


def season_games(season):
    j = get_json(f'{API}/v1/schedule?sportId=1&season={season}&gameType=R&hydrate=linescore&fields='
                 'dates,date,games,gamePk,gameType,status,detailedState,doubleHeader,gameNumber,officialDate')
    out = []
    for d in j.get('dates', []):
        for g in d.get('games', []):
            if g.get('status', {}).get('detailedState') in ('Final', 'Completed Early'):
                out.append((g['gamePk'], g.get('officialDate') or d['date'], g.get('doubleHeader', 'N'), g.get('gameNumber', 1)))
    return out


def parse_game(pk, date, dh, gnum, season):
    feed = get_json(f'{API}/v1.1/game/{pk}/feed/live')
    gd = feed.get('gameData', {}); ld = feed.get('liveData', {})
    teams = gd.get('teams', {})
    abbr = {s: teams.get(s, {}).get('abbreviation') for s in ('home', 'away')}
    tid = {s: teams.get(s, {}).get('id') for s in ('home', 'away')}
    box = ld.get('boxscore', {}).get('teams', {})
    plays = ld.get('plays', {}).get('allPlays', []) or []
    line = ld.get('linescore', {})
    weather = gd.get('weather', {}) or {}
    dt = gd.get('datetime', {}) or {}
    hp_ump = ''
    for o in ld.get('boxscore', {}).get('officials', []) or []:
        if o.get('officialType') == 'Home Plate':
            hp_ump = (o.get('official') or {}).get('fullName', '')

    # ---------- 타석 ----------
    pa_rows = []
    agg = {s: {'pa': 0, 'risp_pa': 0, 'risp_k': 0, 'risp_bb': 0, 'risp_hits': 0, 'risp_runs': 0, 'men_on_pa': 0,
               'empty_pa': 0, 'two_strike_pa': 0, 'two_strike_k': 0, 'runs_1st3': 0, 'runs_last3': 0} for s in ('home', 'away')}
    prev_half = None; outs_before = 0; away_b = 0; home_b = 0
    pitch_no = {}           # pitcher_id -> 누적 투구수
    sp_ids = {}             # side -> starter id (첫 투수)
    hand_of = {}            # pitcher_id -> 투구 손 (matchup 에서; boxscore 에는 없음)
    for p in plays:
        ab = p.get('about', {}); res = p.get('result', {}); mu = p.get('matchup', {}); cnt = p.get('count', {})
        if res.get('type') != 'atBat' and not res.get('eventType'):
            continue
        half = ab.get('halfInning'); inning = ab.get('inning')
        key = (inning, half)
        if key != prev_half:
            outs_before = 0; prev_half = key
        bat_side = 'away' if half == 'top' else 'home'; pit_side = 'home' if bat_side == 'away' else 'away'
        batter = mu.get('batter', {}) or {}; pitcher = mu.get('pitcher', {}) or {}
        pid = pitcher.get('id')
        if pit_side not in sp_ids and pid:
            sp_ids[pit_side] = pid
        npitch = sum(1 for e in p.get('playEvents', []) or [] if e.get('isPitch'))
        pitch_no[pid] = pitch_no.get(pid, 0) + npitch
        if pid and (mu.get('pitchHand') or {}).get('code'):
            hand_of[pid] = mu['pitchHand']['code']
        men_on = ((mu.get('splits') or {}).get('menOnBase')) or ''
        et = res.get('eventType') or ''; ev = res.get('event') or ''
        is_k = et in ('strikeout', 'strikeout_double_play'); is_bb = et in ('walk', 'intent_walk')
        is_hit = et in ('single', 'double', 'triple', 'home_run'); is_hr = et == 'home_run'
        a_after = res.get('awayScore'); h_after = res.get('homeScore')
        runs = 0
        if a_after is not None and h_after is not None:
            runs = (a_after - away_b) + (h_after - home_b)
        two_strike = any((e.get('count') or {}).get('strikes') == 2 for e in p.get('playEvents', []) or [] if e.get('isPitch'))
        row = dict(season=season, date=date, gamePk=pk, inning=inning, half=half, bat_team=abbr[bat_side], pit_team=abbr[pit_side],
                   batter_id=batter.get('id'), batter=batter.get('fullName'), bat_side=(mu.get('batSide') or {}).get('code'),
                   pitcher_id=pid, pitcher=pitcher.get('fullName'), pitch_hand=(mu.get('pitchHand') or {}).get('code'),
                   pitcher_is_sp=int(sp_ids.get(pit_side) == pid), pitcher_pitch_no=pitch_no.get(pid),
                   outs_before=outs_before, men_on=men_on, away_score_before=away_b, home_score_before=home_b,
                   balls=cnt.get('balls'), strikes=cnt.get('strikes'), n_pitches=npitch, event=ev, event_type=et,
                   is_k=int(is_k), is_bb=int(is_bb), is_hit=int(is_hit), is_hr=int(is_hr), is_out=int(bool(res.get('isOut'))),
                   rbi=res.get('rbi'), runs_scored=runs, two_strike=int(two_strike))
        pa_rows.append(row)
        A = agg[bat_side]; A['pa'] += 1
        if men_on == 'RISP' or men_on == 'Loaded':
            A['risp_pa'] += 1; A['risp_k'] += is_k; A['risp_bb'] += is_bb; A['risp_hits'] += is_hit; A['risp_runs'] += runs
        if men_on == 'Empty':
            A['empty_pa'] += 1
        elif men_on:
            A['men_on_pa'] += 1
        if two_strike:
            A['two_strike_pa'] += 1; A['two_strike_k'] += is_k
        if inning and inning <= 3:
            A['runs_1st3'] += runs
        if inning and inning >= 7:
            A['runs_last3'] += runs
        # 상태 갱신
        if cnt.get('outs') is not None:
            outs_before = cnt['outs']
        if a_after is not None:
            away_b = a_after
        if h_after is not None:
            home_b = h_after

    # ---------- 투수 등판 / 팀 박스 ----------
    pit_rows = []; game_rows = []
    for side in ('home', 'away'):
        opp = 'away' if side == 'home' else 'home'
        tb = box.get(side, {}); players = tb.get('players', {}) or {}
        bat = (tb.get('teamStats') or {}).get('batting', {}) or {}
        order = tb.get('pitchers', []) or []
        sp_stats = {}; bp_p = 0; bp_er = 0; bp_outs = 0
        for i, pid in enumerate(order):
            pl = players.get(f'ID{pid}', {}); st = (pl.get('stats') or {}).get('pitching', {}) or {}
            person = pl.get('person', {}) or {}
            outs = ip_to_outs(st.get('inningsPitched'))
            is_sp = int(i == 0)
            dec = 'W' if st.get('wins') else 'L' if st.get('losses') else 'SV' if st.get('saves') else 'H' if st.get('holds') else 'BS' if st.get('blownSaves') else ''
            pit_rows.append(dict(season=season, date=date, gamePk=pk, team=abbr[side], pitcher_id=pid, pitcher=person.get('fullName'),
                                 hand=hand_of.get(pid, ''),
                                 order=i + 1, is_starter=is_sp, ip=st.get('inningsPitched'), outs=outs, pitches=st.get('numberOfPitches') or st.get('pitchesThrown'),
                                 strikes=st.get('strikes'), h=st.get('hits'), er=st.get('earnedRuns'), r=st.get('runs'), k=st.get('strikeOuts'),
                                 bb=st.get('baseOnBalls'), hr=st.get('homeRuns'), bf=st.get('battersFaced'), decision=dec))
            if is_sp:
                sp_stats = dict(sp_id=pid, sp_name=person.get('fullName'), sp_ip=st.get('inningsPitched'), sp_outs=outs,
                                sp_pitches=st.get('numberOfPitches') or st.get('pitchesThrown'), sp_er=st.get('earnedRuns'),
                                sp_k=st.get('strikeOuts'), sp_bb=st.get('baseOnBalls'), sp_h=st.get('hits'))
            else:
                bp_p += (st.get('numberOfPitches') or st.get('pitchesThrown') or 0); bp_er += (st.get('earnedRuns') or 0); bp_outs += (outs or 0)
        # 선발 라인업 / 포수
        lineup = []
        for pid_key, pl in players.items():
            bo = pl.get('battingOrder')
            if bo and str(bo).endswith('00'):
                lineup.append((int(bo), pl))
        lineup.sort()
        cat_id = cat_name = ''
        for _, pl in lineup:
            if (pl.get('position') or {}).get('abbreviation') == 'C':
                cat_id = (pl.get('person') or {}).get('id'); cat_name = (pl.get('person') or {}).get('fullName'); break
        ts = (line.get('teams') or {}).get(side, {}); to = (line.get('teams') or {}).get(opp, {})
        my, th = ts.get('runs'), to.get('runs')
        sp_hand = hand_of.get(sp_stats.get('sp_id'), '')
        row = dict(season=season, date=date, gamePk=pk, game_type='R', doubleheader=dh, game_num=gnum,
                   team=abbr[side], team_id=tid[side], opp=abbr[opp], opp_id=tid[opp], home=int(side == 'home'),
                   team_score=my, opp_score=th, won=(None if my is None or th is None or my == th else int(my > th)),
                   innings=line.get('currentInning'), day_night=dt.get('dayNight'), start_time_utc=dt.get('dateTime'),
                   venue=(gd.get('venue') or {}).get('name'), temp_f=weather.get('temp'), wind=weather.get('wind'),
                   condition=weather.get('condition'), hp_umpire=hp_ump, sp_hand=sp_hand,
                   pitchers_used=len(order), bp_pitches=bp_p, bp_er=bp_er, bp_outs=bp_outs,
                   ab=bat.get('atBats'), r=bat.get('runs'), h=bat.get('hits'), doubles=bat.get('doubles'), triples=bat.get('triples'),
                   hr=bat.get('homeRuns'), rbi=bat.get('rbi'), bb=bat.get('baseOnBalls'), so=bat.get('strikeOuts'), hbp=bat.get('hitByPitch'),
                   sb=bat.get('stolenBases'), cs=bat.get('caughtStealing'), lob=bat.get('leftOnBase'), gidp=bat.get('groundIntoDoublePlay'),
                   catcher_id=cat_id, catcher_name=cat_name,
                   lineup_ids='|'.join(str((pl.get('person') or {}).get('id')) for _, pl in lineup),
                   lineup_pos='|'.join(str((pl.get('position') or {}).get('abbreviation')) for _, pl in lineup))
        row.update(sp_stats); row.update(agg[side])
        game_rows.append(row)
    return game_rows, pa_rows, pit_rows


class SeasonWriter:
    def __init__(self, season):
        self.season = season
        self.done_path = os.path.join(OUT, f'_done_{season}.json')
        self.done = set(json.load(open(self.done_path))) if os.path.exists(self.done_path) else set()
        mode = 'at' if self.done else 'wt'
        self.f = {}
        for name, cols in (('games', GAME_COLS), ('pa', PA_COLS), ('pitching', PIT_COLS)):
            p = os.path.join(OUT, f'{name}_{season}.csv.gz')
            fresh = mode == 'wt' or not os.path.exists(p)
            fh = gzip.open(p, 'wt' if fresh else 'at', encoding='utf-8', newline='')
            w = csv.DictWriter(fh, fieldnames=cols, extrasaction='ignore')
            if fresh:
                w.writeheader()
            self.f[name] = (fh, w)
        self.n = 0

    def write(self, pk, g, pa, pit):
        with LOCK:
            for r in g: self.f['games'][1].writerow(r)
            for r in pa: self.f['pa'][1].writerow(r)
            for r in pit: self.f['pitching'][1].writerow(r)
            self.done.add(pk); self.n += 1
            if self.n % 50 == 0:
                self.flush()

    def flush(self):
        for fh, _ in self.f.values():
            fh.flush()
        json.dump(sorted(self.done), open(self.done_path, 'w'))

    def close(self):
        self.flush()
        for fh, _ in self.f.values():
            fh.close()


def build_season(season, workers, limit=0):
    games = season_games(season)
    w = SeasonWriter(season)
    todo = [g for g in games if g[0] not in w.done]
    if limit:
        todo = todo[:limit]
    log(f'{season}: 정규시즌 Final {len(games)}경기, 이미 완료 {len(w.done)}, 남은 {len(todo)}')
    fails = []
    def job(g):
        pk, date, dh, gnum = g
        try:
            gr, pa, pit = parse_game(pk, date, dh, gnum, season)
            if len(gr) != 2 or not pa:
                raise ValueError(f'파싱 결과 이상 games={len(gr)} pa={len(pa)}')
            w.write(pk, gr, pa, pit)
        except Exception as e:
            fails.append((pk, date, str(e)[:120]))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(job, g) for g in todo]
        for i, _ in enumerate(as_completed(futs), 1):
            if i % 200 == 0:
                log(f'{season}: {i}/{len(todo)}')
    w.close()
    if fails:
        json.dump(fails, open(os.path.join(OUT, f'_fail_{season}.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    log(f'{season}: 완료 {len(w.done)}경기, 실패 {len(fails)}건' + (' → _fail 파일 확인' if fails else ''))
    return len(w.done), len(fails)


BANNER = '''
============================================================
  MLB 데이터 창고 구축  (2015~올해, StatsAPI 타석 단위)
============================================================
  1. 연기 테스트 - 올해 20경기만 받아서 파싱 확인 (1분)
  2. 본 수집 - 2015~올해 정규시즌 전 경기 (약 27,000경기, 1~3시간)
     경기당 팀별 1줄 / 타석당 1줄 / 투수 등판당 1줄 -> data\\warehouse\\
  3. GitHub 에 올리기
  * 사장님 PC 에서만 됩니다 (Claude 샌드박스는 StatsAPI 차단)
  * 중간에 닫아도 다시 누르면 이어서 받습니다. 배당은 수집하지 않습니다.
'''


def cmd_check(min_games):
    import glob
    n = 0
    for f in glob.glob(os.path.join(OUT, 'games_*.csv.gz')):
        with gzip.open(f, 'rt', encoding='utf-8') as fh:
            n += sum(1 for _ in csv.DictReader(fh))
    print(f'      games 행 {n} (팀별 1줄, {min_games}경기면 {min_games*2})')
    return 0 if n >= min_games else 9


def cmd_summary():
    p = os.path.join(OUT, '_manifest.json')
    if not os.path.exists(p):
        print('      _manifest.json 없음'); return 9
    m = json.load(open(p, encoding='utf-8'))
    for k, v in m['seasons'].items():
        print(f'      {k}: {v[0]}경기 / 실패 {v[1]}')
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--seasons', default=f'2015-{datetime.now().year}')
    ap.add_argument('--workers', type=int, default=6)
    ap.add_argument('--limit', type=int, default=0, help='시즌당 최대 경기 수 (연기 테스트용)')
    ap.add_argument('--banner', action='store_true'); ap.add_argument('--check', type=int, default=0); ap.add_argument('--summary', action='store_true')
    a = ap.parse_args()
    if a.banner:
        print(BANNER); return 0
    os.makedirs(OUT, exist_ok=True)
    if a.check:
        return cmd_check(a.check)
    if a.summary:
        return cmd_summary()
    s0, _, s1 = a.seasons.partition('-')
    seasons = list(range(int(s0), int(s1 or s0) + 1))
    log(f'창고 구축 시작 seasons={seasons} workers={a.workers} out={OUT}')
    try:
        get_json(f'{API}/v1/teams?sportId=1&fields=teams,id')
    except Exception as e:
        log(f'[FATAL] StatsAPI 접속 불가: {e} — 이 스크립트는 사장님 PC 에서만 돈다')
        return 9
    summary = {}
    for s in seasons:
        try:
            summary[s] = build_season(s, a.workers, a.limit)
        except Exception as e:
            log(f'[ERROR] {s}: {e}')
            summary[s] = (None, None)
    json.dump({'built_at': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'seasons': {str(k): v for k, v in summary.items()},
               'source': 'MLB StatsAPI live feed', 'odds_collected': False},
              open(os.path.join(OUT, '_manifest.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    log('전부 완료. data/warehouse/_manifest.json 참고')
    return 0


if __name__ == '__main__':
    sys.exit(main())
