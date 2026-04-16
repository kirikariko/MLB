"""
MLB KING v2 — 84-Column Game Data Collector
MLB Stats API + FanGraphs API

Covers all columns from MLB TWO.xlsx:
  Col 1-36:  Home Team block
  Col 37-72: Away Team block (mirror)
  Col 73-84: Game meta (series, weather, park factor, results, date, game_id)
"""

import math
import os
import requests
import pandas as pd
import time
import json
import sys
from datetime import datetime, timedelta, timezone

# ============================================================
# CONFIG
# ============================================================

MLB_TEAM_IDS = {
    'ARI': 109, 'ATL': 144, 'BAL': 110, 'BOS': 111, 'CHC': 112, 'CWS': 145,
    'CIN': 113, 'CLE': 114, 'COL': 115, 'DET': 116, 'HOU': 117, 'KCR': 118,
    'LAA': 108, 'LAD': 119, 'MIA': 146, 'MIL': 158, 'MIN': 142, 'NYM': 121,
    'NYY': 147, 'OAK': 133, 'PHI': 143, 'PIT': 134, 'SDP': 135, 'SEA': 136,
    'SFG': 137, 'STL': 138, 'TBR': 139, 'TEX': 140, 'TOR': 141, 'WSN': 120
}

# MLB API team ID -> our standard abbreviation
MLB_ID_TO_ABBR = {v: k for k, v in MLB_TEAM_IDS.items()}

# FanGraphs abbreviation -> our standard abbreviation
# Most are the same; only ATH->OAK and CHW->CWS differ
FG_ABBR_TO_OURS = {
    'ATH': 'OAK',
    'CHW': 'CWS',
}
# All other FG abbrs (ARI, ATL, BAL, ...) are identical to ours

# The Odds API key (for O/U lines)
ODDS_API_KEY = 'a6278de71ea0fdc834750e829e8f14bc'

# The Odds API team name -> our abbreviation
ODDS_NAME_TO_ABBR = {
    'Arizona Diamondbacks': 'ARI', 'Atlanta Braves': 'ATL',
    'Baltimore Orioles': 'BAL', 'Boston Red Sox': 'BOS',
    'Chicago Cubs': 'CHC', 'Chicago White Sox': 'CWS',
    'Cincinnati Reds': 'CIN', 'Cleveland Guardians': 'CLE',
    'Colorado Rockies': 'COL', 'Detroit Tigers': 'DET',
    'Houston Astros': 'HOU', 'Kansas City Royals': 'KCR',
    'Los Angeles Angels': 'LAA', 'Los Angeles Dodgers': 'LAD',
    'Miami Marlins': 'MIA', 'Milwaukee Brewers': 'MIL',
    'Minnesota Twins': 'MIN', 'New York Mets': 'NYM',
    'New York Yankees': 'NYY', 'Athletics': 'OAK',
    'Philadelphia Phillies': 'PHI', 'Pittsburgh Pirates': 'PIT',
    'San Diego Padres': 'SDP', 'San Francisco Giants': 'SFG',
    'Seattle Mariners': 'SEA', 'St. Louis Cardinals': 'STL',
    'Tampa Bay Rays': 'TBR', 'Texas Rangers': 'TEX',
    'Toronto Blue Jays': 'TOR', 'Washington Nationals': 'WSN',
}

# Stadium coordinates for weather API (Open-Meteo)
STADIUM_COORDINATES = {
    'ARI': {'lat': 33.45, 'lon': -112.07},
    'ATL': {'lat': 33.89, 'lon': -84.47},
    'BAL': {'lat': 39.28, 'lon': -76.62},
    'BOS': {'lat': 42.35, 'lon': -71.09},
    'CHC': {'lat': 41.95, 'lon': -87.66},
    'CWS': {'lat': 41.83, 'lon': -87.63},
    'CIN': {'lat': 39.09, 'lon': -84.51},
    'CLE': {'lat': 41.49, 'lon': -81.68},
    'COL': {'lat': 39.75, 'lon': -104.99},
    'DET': {'lat': 42.34, 'lon': -83.05},
    'HOU': {'lat': 29.75, 'lon': -95.35},
    'KCR': {'lat': 39.05, 'lon': -94.48},
    'LAA': {'lat': 33.80, 'lon': -117.88},
    'LAD': {'lat': 34.07, 'lon': -118.24},
    'MIA': {'lat': 25.78, 'lon': -80.22},
    'MIL': {'lat': 43.03, 'lon': -87.97},
    'MIN': {'lat': 44.98, 'lon': -93.28},
    'NYM': {'lat': 40.76, 'lon': -73.84},
    'NYY': {'lat': 40.83, 'lon': -73.93},
    'OAK': {'lat': 37.75, 'lon': -122.20},
    'PHI': {'lat': 39.90, 'lon': -75.17},
    'PIT': {'lat': 40.45, 'lon': -80.01},
    'SDP': {'lat': 32.71, 'lon': -117.16},
    'SEA': {'lat': 47.59, 'lon': -122.33},
    'SFG': {'lat': 37.78, 'lon': -122.39},
    'STL': {'lat': 38.62, 'lon': -90.20},
    'TBR': {'lat': 27.77, 'lon': -82.65},
    'TEX': {'lat': 32.75, 'lon': -97.08},
    'TOR': {'lat': 43.64, 'lon': -79.39},
    'WSN': {'lat': 38.87, 'lon': -77.01},
}

# Stadium timezone offsets (standard UTC offset — used for timezone change calc)
STADIUM_TIMEZONE = {
    # Eastern (UTC-5)
    'NYY': -5, 'NYM': -5, 'BOS': -5, 'BAL': -5, 'PHI': -5, 'WSN': -5,
    'MIA': -5, 'TBR': -5, 'ATL': -5, 'CIN': -5, 'CLE': -5, 'DET': -5, 'PIT': -5, 'TOR': -5,
    # Central (UTC-6)
    'CHC': -6, 'CWS': -6, 'MIN': -6, 'MIL': -6, 'STL': -6, 'KCR': -6, 'HOU': -6, 'TEX': -6,
    # Mountain (UTC-7)
    'ARI': -7, 'COL': -7,
    # Pacific (UTC-8)
    'LAD': -8, 'LAA': -8, 'SDP': -8, 'SFG': -8, 'SEA': -8, 'OAK': -8,
}


def haversine_miles(lat1, lon1, lat2, lon2):
    """Calculate great-circle distance between two points in miles."""
    R = 3959  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


# columns in exact order matching MLB TWO.xlsx (84 original + 10 new)
COLUMNS = [
    # Home Team (1-36)
    'SWP_HOMETEAM', 'SAPS_HOMETEAM', 'SAPA_HOMETEAM',
    'Home_Win_Rate', 'SAPS_HOME_ONLY_HOMETEAM', 'SAPA_HOME_ONLY_HOMETEAM',
    'L5G_WP_HOMETEAM', "Last 5 games' avg. runs_Home Team", "Last 5 games' avg. runs conceded_Home Team",
    'R_H2H_WP_HOMETEAM',
    'ERA_HOMETEAM_PITCHER',
    'AVG_BATTING_AVERAGE_HOMETEAM_Total',
    'AVG_BATTING_AVERAGE_HOMETEAM_VS LEFTPitcher',
    'AVG_BATTING_AVERAGE_HOMETEAM_VS RightPitcher',
    'AVG_BATTING_AVERAGE_HOMETEAM_HOMEONLY',
    'ERA__HOMETEAMPITCHER_HOMEONLY',
    'AVG_BATTING_AVERAGE_HOMETEAM_DAY_GAME',
    'ERA__HOMETEAMPITCHER_DAYGAME',
    'AVG_BATTING_AVERAGE_HOMETEAM_NIGHT_GAME',
    'ERA__HOMETEAMPITCHER_NIGHTGAME',
    'On Base%_HOMETEAM', 'ERA_HOMETEAM',
    'Home_Team_PM', 'Home_Team_BM',
    'HOMETEAM_Woba', 'HOMETEAM_SIERA',
    'BP_ERA_30d_HOME', 'BP_IRS_30d_HOME', 'BP_IP_3d_HOME',
    'HOMETEAM_BULLPEN_WORKLOAD',
    'HOMETEAM_FPct', 'HOMETEAM_E',
    'HOMETEAM_BvP_AB ', 'HOMETEAM_BvP_AVG', 'HOMETEAM_BvP_OBP', 'HOMETEAM_BvP_CONF',
    # Away Team (37-72)
    'SWP_AWAYTEAM', 'SAPS_AWAYTEAM', 'SAPA_AWAYTEAM',
    'AwayTEAM_Win_Rate_ONLYAWAY', 'SAPS_AWAY_ONLY_AWAYTEAM', 'SAPA_AWAY_ONLY_AWAYTEAM',
    'L5G_WP_AWAYTEAM', "Last 5 games' avg. runs_AWAY Team", "Last 5 games' avg. runs conceded_AWAY Team",
    'R_H2H_WP_AWAYTEAM',
    'ERA_AWAYTEAM_PITCHER',
    'AVG_BATTING_AVERAGE_AWAYTEAM_Total',
    'AVG_BATTING_AVERAGE_AWAYTEAM_VS LEFTPitcher',
    'AVG_BATTING_AVERAGE_AWAYTEAM_VS RightPitcher',
    'AVG_BATTING_AVERAGE_AWAYTEAM_AWAYONLY',
    'ERA__AWAYTEAMPITCHER_AWAYONLY',
    'AVG_BATTING_AVERAGE_AWAYTEAM_DAY_GAME',
    'ERA__AWAYTEAMPITCHER_DAYGAME',
    'AVG_BATTING_AVERAGE_AWAYTEAM_NIGHT_GAME',
    'ERA__AWAYTEAMPITCHER_NIGHTGAME',
    'On Base%_AWAYTEAM', 'ERA_AWAYTEAM',
    'AWAYTEAM_PM', 'AWAYTEAM_BM',
    'AWAYTEAM_wOBA', 'AWAYTEAM_SIERA',
    'BP_ERA_30d_AWAY', 'BP_IRS_30d_AWAY', 'BP_IP_3d_AWAY',
    'AWAYTEAM_BULLPEN_WORKLOAD',
    'AWAYTEAM_FPct', 'AWAYTEAM_E',
    'AWAYTEAM_BvP_AB ', 'AWAYTEAM_BvP_AVG', 'AWAYTEAM_BvP_OBP', 'AWAYTEAM_BvP_CONF',
    # Game Meta (73-84)
    'Series_Game_Num', 'Series_Home_Wins', 'Series_Away_Wins',
    'U/O_LINE',
    'Avg_Temp_C', 'Precipitation_mm', 'Max_Wind_kph', 'Wind_Dir_Deg',
    'PARK_FACTOR',
    # Umpire (85-86)
    'UMP_NAME', 'UMP_RPG',
    # Travel/Fatigue — Home (87-89)
    'HOME_TRAVEL_MI', 'HOME_REST_DAYS', 'HOME_ROAD_STREAK',
    # Travel/Fatigue — Away (90-92)
    'AWAY_TRAVEL_MI', 'AWAY_REST_DAYS', 'AWAY_ROAD_STREAK',
    # Situational Tags (93-94)
    'HOME_SIT_TAG', 'AWAY_SIT_TAG',
    # SP Statcast (95-102)
    'HOME_SP_xwOBA', 'HOME_SP_HardHit', 'HOME_SP_Barrel', 'HOME_SP_PitchMix',
    'AWAY_SP_xwOBA', 'AWAY_SP_HardHit', 'AWAY_SP_Barrel', 'AWAY_SP_PitchMix',
    # Results + ID (103-106)
    'U/O_RESULT', 'Winning Team',
    'DATE', 'GAME_ID'
]


# ============================================================
# MLB STATS API
# ============================================================

class MLBApi:
    """MLB Stats API wrapper with caching."""
    BASE = "https://statsapi.mlb.com/api/v1"

    def __init__(self, year):
        self.year = year
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'MLBKing/2.0'})
        # Caches
        self._standings = None
        self._schedules = {}       # team_id -> list of games
        self._team_stats = {}      # (team_id, group, sitCode) -> stats dict
        self._player_stats = {}    # (player_id, group, sitCode) -> stats dict
        self._fielding = {}        # team_id -> stats dict

    def _get(self, url, params=None):
        """Make GET request with retry."""
        for attempt in range(3):
            try:
                r = self.session.get(url, params=params, timeout=15)
                r.raise_for_status()
                return r.json()
            except requests.exceptions.RequestException as e:
                if attempt == 2:
                    print(f"  [API ERROR] {url}: {e}")
                    return None
                time.sleep(1)
        return None

    # --- Standings ---
    def get_standings(self):
        """Get all team season records + split records (home/away/day/night)."""
        if self._standings:
            return self._standings

        data = self._get(f"{self.BASE}/standings", {
            'leagueId': '103,104',
            'season': self.year,
            'hydrate': 'team',
            'standingsTypes': 'regularSeason'
        })
        if not data:
            return {}

        result = {}
        for record in data.get('records', []):
            for tr in record.get('teamRecords', []):
                team_id = tr['team']['id']
                abbr = MLB_ID_TO_ABBR.get(team_id, '???')
                gp = tr.get('gamesPlayed', 0) or (tr.get('wins', 0) + tr.get('losses', 0))

                # Parse split records
                splits = {}
                for sr in tr.get('records', {}).get('splitRecords', []):
                    splits[sr['type']] = {
                        'wins': sr.get('wins', 0),
                        'losses': sr.get('losses', 0),
                        'pct': float(sr.get('pct', '0') or '0'),
                    }

                # Parse gamesBack ("-" means leader, else numeric string)
                def _parse_gb(raw):
                    if raw is None or raw == '-':
                        return 0.0
                    try:
                        return float(str(raw).replace('+', ''))
                    except (ValueError, TypeError):
                        return None

                result[team_id] = {
                    'abbr': abbr,
                    'wins': tr.get('wins', 0),
                    'losses': tr.get('losses', 0),
                    'gp': gp,
                    'win_pct': float(tr.get('winningPercentage', '0') or '0'),
                    'runs_scored': tr.get('runsScored', 0),
                    'runs_allowed': tr.get('runsAllowed', 0),
                    'splits': splits,
                    # Dept3-2 additions
                    'division_rank': int(tr.get('divisionRank', 0) or 0) or None,
                    'games_back': _parse_gb(tr.get('gamesBack')),
                    'wildcard_gb': _parse_gb(tr.get('wildCardGamesBack')),
                    'streak': tr.get('streak', {}).get('streakCode'),
                }

        self._standings = result
        return result

    # --- Schedule ---
    def get_schedule(self, team_id):
        """Get full season schedule for a team (completed games only)."""
        if team_id in self._schedules:
            return self._schedules[team_id]

        data = self._get(f"{self.BASE}/schedule", {
            'sportId': 1, 'teamId': team_id,
            'season': self.year, 'gameType': 'R',
            'hydrate': 'probablePitcher,linescore'
        })
        if not data:
            return []

        games = []
        for date_data in data.get('dates', []):
            for game in date_data.get('games', []):
                games.append({
                    'game_pk': game.get('gamePk'),
                    'date': date_data['date'],
                    'status': game.get('status', {}).get('abstractGameState'),
                    'home_id': game['teams']['home']['team']['id'],
                    'away_id': game['teams']['away']['team']['id'],
                    'home_score': game['teams']['home'].get('score', 0),
                    'away_score': game['teams']['away'].get('score', 0),
                    'home_pitcher_id': game['teams']['home'].get('probablePitcher', {}).get('id'),
                    'away_pitcher_id': game['teams']['away'].get('probablePitcher', {}).get('id'),
                    'home_pitcher_name': game['teams']['home'].get('probablePitcher', {}).get('fullName'),
                    'away_pitcher_name': game['teams']['away'].get('probablePitcher', {}).get('fullName'),
                    'day_night': game.get('dayNight', 'night'),
                })
        self._schedules[team_id] = games
        return games

    # --- Today's Games ---
    def get_today_games(self, date_str):
        """Get today's scheduled games with probable pitchers."""
        data = self._get(f"{self.BASE}/schedule", {
            'sportId': 1, 'date': date_str,
            'gameType': 'R',
            'hydrate': 'probablePitcher,linescore,team,officials'
        })
        if not data:
            return []

        games = []
        for date_data in data.get('dates', []):
            for game in date_data.get('games', []):
                home_team = game['teams']['home']['team']
                away_team = game['teams']['away']['team']
                games.append({
                    'game_pk': game.get('gamePk'),
                    'date': date_data['date'],
                    'game_time': game.get('gameDate', ''),
                    'status': game.get('status', {}).get('abstractGameState'),
                    'day_night': game.get('dayNight', 'night'),
                    'home_id': home_team['id'],
                    'away_id': away_team['id'],
                    'home_abbr': MLB_ID_TO_ABBR.get(home_team['id'], home_team.get('abbreviation', '???')),
                    'away_abbr': MLB_ID_TO_ABBR.get(away_team['id'], away_team.get('abbreviation', '???')),
                    'home_pitcher_id': game['teams']['home'].get('probablePitcher', {}).get('id'),
                    'away_pitcher_id': game['teams']['away'].get('probablePitcher', {}).get('id'),
                    'home_pitcher_name': game['teams']['home'].get('probablePitcher', {}).get('fullName', 'TBD'),
                    'away_pitcher_name': game['teams']['away'].get('probablePitcher', {}).get('fullName', 'TBD'),
                    'venue': game.get('venue', {}).get('name', ''),
                })
                # Extract home plate umpire
                for off in game.get('officials', []):
                    if off.get('officialType') == 'Home Plate':
                        games[-1]['ump_id'] = off['official']['id']
                        games[-1]['ump_name'] = off['official']['fullName']
                        break
                else:
                    games[-1]['ump_id'] = None
                    games[-1]['ump_name'] = None
        return games

    # --- Team Hitting/Pitching Stats ---
    def get_team_stats(self, team_id, group='hitting', sit_code=None):
        """Get team batting or pitching stats. sit_code: h/a/d/n/vl/vr or None for season."""
        cache_key = (team_id, group, sit_code)
        if cache_key in self._team_stats:
            return self._team_stats[cache_key]

        params = {
            'stats': 'statSplits' if sit_code else 'season',
            'group': group,
            'season': self.year,
        }
        if sit_code:
            params['sitCodes'] = sit_code

        data = self._get(f"{self.BASE}/teams/{team_id}/stats", params)
        if not data:
            return {}

        stats = {}
        for split_group in data.get('stats', []):
            for split in split_group.get('splits', []):
                stats = split.get('stat', {})
                break  # Take first split
            break

        self._team_stats[cache_key] = stats
        time.sleep(0.2)
        return stats

    # --- Player Pitching Stats ---
    def get_pitcher_stats(self, pitcher_id, sit_code=None):
        """Get individual pitcher stats. sit_code: h/a/d/n or None for season."""
        if not pitcher_id:
            return {}

        cache_key = (pitcher_id, 'pitching', sit_code)
        if cache_key in self._player_stats:
            return self._player_stats[cache_key]

        params = {
            'stats': 'statSplits' if sit_code else 'season',
            'group': 'pitching',
            'season': self.year,
        }
        if sit_code:
            params['sitCodes'] = sit_code

        data = self._get(f"{self.BASE}/people/{pitcher_id}/stats", params)
        if not data:
            return {}

        stats = {}
        for split_group in data.get('stats', []):
            for split in split_group.get('splits', []):
                stats = split.get('stat', {})
                break
            break

        self._player_stats[cache_key] = stats
        time.sleep(0.2)
        return stats

    # --- Team Fielding ---
    def get_team_fielding(self, team_id):
        """Get team fielding stats (FPct, errors)."""
        if team_id in self._fielding:
            return self._fielding[team_id]

        data = self._get(f"{self.BASE}/teams/{team_id}/stats", {
            'stats': 'season',
            'group': 'fielding',
            'season': self.year,
        })
        if not data:
            return {}

        stats = {}
        for split_group in data.get('stats', []):
            for split in split_group.get('splits', []):
                stats = split.get('stat', {})
                break
            break

        self._fielding[team_id] = stats
        time.sleep(0.2)
        return stats

    # --- Last 7 Days Stats (byDateRange) ---
    def get_team_stats_last7d(self, team_id, group='hitting'):
        """Get team stats for last 7 days using byDateRange."""
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        cache_key = (team_id, group, f'last7d_{end_date}')
        if cache_key in self._team_stats:
            return self._team_stats[cache_key]

        data = self._get(f"{self.BASE}/teams/{team_id}/stats", {
            'stats': 'byDateRange',
            'group': group,
            'season': self.year,
            'startDate': start_date,
            'endDate': end_date,
        })
        if not data:
            return {}

        stats = {}
        for split_group in data.get('stats', []):
            for split in split_group.get('splits', []):
                stats = split.get('stat', {})
                break
            break

        self._team_stats[cache_key] = stats
        time.sleep(0.2)
        return stats

    def get_pitcher_stats_last7d(self, pitcher_id):
        """Get individual pitcher stats for last 7 days."""
        if not pitcher_id:
            return {}

        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        cache_key = (pitcher_id, 'pitching', f'last7d_{end_date}')
        if cache_key in self._player_stats:
            return self._player_stats[cache_key]

        data = self._get(f"{self.BASE}/people/{pitcher_id}/stats", {
            'stats': 'byDateRange',
            'group': 'pitching',
            'season': self.year,
            'startDate': start_date,
            'endDate': end_date,
        })
        if not data:
            return {}

        stats = {}
        for split_group in data.get('stats', []):
            for split in split_group.get('splits', []):
                stats = split.get('stat', {})
                break
            break

        self._player_stats[cache_key] = stats
        time.sleep(0.2)
        return stats

    # --- Boxscore (for bullpen stats) ---
    def get_boxscore(self, game_pk):
        """Get game boxscore with pitcher details."""
        cache_key = f"box_{game_pk}"
        if cache_key in self._player_stats:
            return self._player_stats[cache_key]

        data = self._get(f"{self.BASE}/game/{game_pk}/boxscore")
        if not data:
            return {}

        self._player_stats[cache_key] = data
        time.sleep(0.15)
        return data

    # --- BvP (Batter vs Pitcher) ---
    def get_bvp(self, batter_id, pitcher_id):
        """Get batter vs pitcher career stats."""
        if not batter_id or not pitcher_id:
            return {}

        data = self._get(f"{self.BASE}/people/{batter_id}/stats", {
            'stats': 'vsPlayer',
            'group': 'hitting',
            'opposingPlayerId': pitcher_id,
        })
        if not data:
            return {}

        combined = {'atBats': 0, 'hits': 0, 'baseOnBalls': 0, 'hitByPitch': 0,
                    'plateAppearances': 0, 'avg': '.000', 'obp': '.000'}
        for split_group in data.get('stats', []):
            for split in split_group.get('splits', []):
                s = split.get('stat', {})
                combined['atBats'] += s.get('atBats', 0)
                combined['hits'] += s.get('hits', 0)
                combined['baseOnBalls'] += s.get('baseOnBalls', 0)
                combined['hitByPitch'] += s.get('hitByPitch', 0)
                combined['plateAppearances'] += s.get('plateAppearances', 0)
        if combined['atBats'] > 0:
            combined['avg'] = f"{combined['hits'] / combined['atBats']:.3f}"
        pa = combined['plateAppearances']
        if pa > 0:
            obp_num = combined['hits'] + combined['baseOnBalls'] + combined['hitByPitch']
            combined['obp'] = f"{obp_num / pa:.3f}"
        return combined

    # --- Umpire RPG map (avg total runs per game, all umpires) ---
    def _build_umpire_rpg_map(self):
        """Build umpire_id -> RPG map from previous season (Jun-Sep, ~4 months).
        One-time cost per session, cached after first call."""
        if hasattr(self, '_ump_rpg_map') and self._ump_rpg_map is not None:
            return self._ump_rpg_map

        prev = self.year - 1
        print(f"    [Umpire] Building RPG map from {prev} season (Jun-Sep)...")
        ump_map = {}
        for start_m, end_m in [('06-01', '07-31'), ('08-01', '09-30')]:
            data = self._get(f"{self.BASE}/schedule", {
                'sportId': 1, 'season': prev, 'gameType': 'R',
                'startDate': f'{prev}-{start_m}', 'endDate': f'{prev}-{end_m}',
                'hydrate': 'officials,linescore',
            })
            if not data:
                continue
            for dt in data.get('dates', []):
                for g in dt.get('games', []):
                    ls = g.get('linescore', {})
                    hr = (ls.get('teams', {}).get('home', {}).get('runs') or 0)
                    ar = (ls.get('teams', {}).get('away', {}).get('runs') or 0)
                    total = hr + ar
                    if total == 0:
                        continue
                    for o in g.get('officials', []):
                        if o.get('officialType') == 'Home Plate':
                            uid = o['official']['id']
                            if uid not in ump_map:
                                ump_map[uid] = {'runs': 0, 'games': 0}
                            ump_map[uid]['runs'] += total
                            ump_map[uid]['games'] += 1
                            break

        self._ump_rpg_map = {}
        for uid, u in ump_map.items():
            if u['games'] >= 10:
                self._ump_rpg_map[uid] = round(u['runs'] / u['games'], 2)
        print(f"    [Umpire] RPG map built: {len(self._ump_rpg_map)} umpires ({prev} Jun-Sep)")
        return self._ump_rpg_map

    def get_umpire_rpg(self, umpire_id):
        """Get avg total RPG for a home plate umpire (from previous season)."""
        if not umpire_id:
            return None
        rpg_map = self._build_umpire_rpg_map()
        return rpg_map.get(umpire_id)

    # --- SP throws (L/R) ---
    def get_pitcher_hand(self, pitcher_id):
        """Get pitcher's throwing hand (L or R). Returns None if not found."""
        if not pitcher_id:
            return None
        cache_key = f"hand_{pitcher_id}"
        if cache_key in self._player_stats:
            return self._player_stats[cache_key]
        r = self._get(f"{self.BASE}/people/{pitcher_id}")
        hand = None
        if r and r.get('people'):
            hand = r['people'][0].get('pitchHand', {}).get('code')
        self._player_stats[cache_key] = hand
        return hand

    # --- All Players (for lineup name->ID resolution) ---
    def get_all_players(self):
        """Get all active MLB players for the season. Used to resolve names to IDs.
        Returns a list of {id, fullName, ...} dicts. Cached after first call."""
        if hasattr(self, '_all_players_cache') and self._all_players_cache is not None:
            return self._all_players_cache

        data = self._get(f"{self.BASE}/sports/1/players", {'season': self.year})
        if not data:
            self._all_players_cache = []
            return []

        self._all_players_cache = data.get('people', [])
        return self._all_players_cache

    # --- Roster (for BvP lineup) ---
    def get_roster(self, team_id):
        """Get active roster player IDs."""
        data = self._get(f"{self.BASE}/teams/{team_id}/roster", {
            'rosterType': 'active',
            'season': self.year,
        })
        if not data:
            return []

        players = []
        for p in data.get('roster', []):
            players.append({
                'id': p['person']['id'],
                'name': p['person']['fullName'],
                'position': p.get('position', {}).get('abbreviation', ''),
            })
        return players

    # --- Individual Batter Stats (for lineup-based batting) ---
    def get_batter_stats(self, batter_id, sit_code=None):
        """Get individual batter hitting stats. sit_code: h/a/d/n/vl/vr or None for season."""
        if not batter_id:
            return {}

        cache_key = (batter_id, 'hitting', sit_code)
        if cache_key in self._player_stats:
            return self._player_stats[cache_key]

        params = {
            'stats': 'statSplits' if sit_code else 'season',
            'group': 'hitting',
            'season': self.year,
        }
        if sit_code:
            params['sitCodes'] = sit_code

        data = self._get(f"{self.BASE}/people/{batter_id}/stats", params)
        if not data:
            return {}

        stats = {}
        for split_group in data.get('stats', []):
            for split in split_group.get('splits', []):
                stats = split.get('stat', {})
                break
            break

        self._player_stats[cache_key] = stats
        time.sleep(0.1)
        return stats

    def get_batter_stats_last7d(self, batter_id):
        """Get individual batter hitting stats for last 7 days."""
        if not batter_id:
            return {}

        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        cache_key = (batter_id, 'hitting', f'last7d_{end_date}')
        if cache_key in self._player_stats:
            return self._player_stats[cache_key]

        data = self._get(f"{self.BASE}/people/{batter_id}/stats", {
            'stats': 'byDateRange',
            'group': 'hitting',
            'season': self.year,
            'startDate': start_date,
            'endDate': end_date,
        })
        if not data:
            return {}

        stats = {}
        for split_group in data.get('stats', []):
            for split in split_group.get('splits', []):
                stats = split.get('stat', {})
                break
            break

        self._player_stats[cache_key] = stats
        time.sleep(0.1)
        return stats


# ============================================================
# FANGRAPHS API
# ============================================================

class FanGraphsApi:
    """FanGraphs API for wOBA, SIERA, Park Factors."""
    BASE = "https://www.fangraphs.com/api"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.fangraphs.com/',
        })
        self._batting_cache = None
        self._pitching_cache = None
        self._park_cache = None

    def _get(self, url, params=None):
        for attempt in range(3):
            try:
                r = self.session.get(url, params=params, timeout=15)
                r.raise_for_status()
                return r.json()
            except Exception as e:
                if attempt == 2:
                    print(f"  [FG ERROR] {url}: {e}")
                    return None
                time.sleep(2)
        return None

    def _match_team(self, fg_abbr):
        """Convert FanGraphs team abbreviation to our standard abbreviation."""
        if not fg_abbr:
            return None
        return FG_ABBR_TO_OURS.get(fg_abbr, fg_abbr)

    def get_team_batting(self, year):
        """Get all teams' batting stats including wOBA."""
        if self._batting_cache:
            return self._batting_cache

        data = self._get(f"{self.BASE}/leaders/major-league/data", {
            'pos': 'all', 'stats': 'bat', 'lg': 'all', 'qual': '0',
            'season': year, 'season1': year,
            'month': '0', 'team': '0,ts',
            'pageitems': '50', 'pagenum': '1',
            'ind': '0', 'rost': '0', 'players': '0',
            'type': '8',  # Standard + Advanced (includes wOBA)
        })
        if not data:
            return {}

        result = {}
        rows = data if isinstance(data, list) else data.get('data', [])
        for row in rows:
            fg_abbr = row.get('TeamName', row.get('TeamNameAbb', ''))
            abbr = self._match_team(fg_abbr)
            if abbr:
                result[abbr] = {
                    'wOBA': row.get('wOBA', None),
                    'wRC_plus': row.get('wRC+', None),
                    'BABIP': row.get('BABIP', None),
                }
        self._batting_cache = result
        print(f"  [FanGraphs] Team batting loaded: {len(result)} teams (wOBA)")
        return result

    def get_team_pitching(self, year):
        """Get all teams' pitching stats including SIERA."""
        if self._pitching_cache:
            return self._pitching_cache

        data = self._get(f"{self.BASE}/leaders/major-league/data", {
            'pos': 'all', 'stats': 'pit', 'lg': 'all', 'qual': '0',
            'season': year, 'season1': year,
            'month': '0', 'team': '0,ts',
            'pageitems': '50', 'pagenum': '1',
            'ind': '0', 'rost': '0', 'players': '0',
            'type': '24',  # Advanced pitching (includes SIERA)
        })
        if not data:
            return {}

        result = {}
        rows = data if isinstance(data, list) else data.get('data', [])
        for row in rows:
            fg_abbr = row.get('TeamName', row.get('TeamNameAbb', ''))
            abbr = self._match_team(fg_abbr)
            if abbr:
                result[abbr] = {
                    'SIERA': row.get('SIERA', None),
                    'xFIP': row.get('xFIP', None),
                    'FIP': row.get('FIP', None),
                }
        self._pitching_cache = result
        print(f"  [FanGraphs] Team pitching loaded: {len(result)} teams (SIERA)")
        return result

    def get_park_factors(self, year):
        """Get park factors for all teams."""
        if self._park_cache:
            return self._park_cache

        data = self._get(f"{self.BASE}/guts/park-factors", {
            'season': year,
        })
        if not data:
            # Fallback: hardcoded 2025 park factors
            print("  [FanGraphs] Park factors API failed, using defaults")
            self._park_cache = self._default_park_factors()
            return self._park_cache

        result = {}
        rows = data if isinstance(data, list) else data.get('data', data)
        if isinstance(rows, dict):
            rows = rows.get('records', rows.get('results', []))

        for row in rows:
            fg_abbr = row.get('TeamName', row.get('TeamNameAbb', row.get('name', '')))
            abbr = self._match_team(fg_abbr)
            if abbr:
                # Park factor is usually "basic" or "runs" factor, 100 = neutral
                pf = row.get('Basic', row.get('basic', row.get('PF', row.get('Runs', 100))))
                result[abbr] = pf

        if result:
            self._park_cache = result
            print(f"  [FanGraphs] Park factors loaded: {len(result)} parks")
        else:
            self._park_cache = self._default_park_factors()
            print("  [FanGraphs] Park factors parse failed, using defaults")

        return self._park_cache

    def _default_park_factors(self):
        """Fallback park factors (2025 approximate values)."""
        return {
            'COL': 114, 'ARI': 106, 'CIN': 105, 'TEX': 104, 'BOS': 104,
            'CHC': 103, 'ATL': 102, 'BAL': 102, 'MIL': 101, 'PHI': 101,
            'NYY': 101, 'LAA': 100, 'HOU': 100, 'KCR': 100, 'MIN': 100,
            'STL': 100, 'TOR': 100, 'CWS': 99, 'DET': 99, 'PIT': 99,
            'WSN': 99, 'CLE': 98, 'SDP': 98, 'NYM': 98, 'LAD': 97,
            'SFG': 97, 'TBR': 97, 'SEA': 96, 'MIA': 96, 'OAK': 95,
        }


# ============================================================
# BASEBALL SAVANT (Statcast — xwOBA, barrel%, hard hit%, pitch mix)
# ============================================================

class SavantApi:
    """Baseball Savant CSV leaderboard fetcher for SP Statcast data."""

    BASE = "https://baseballsavant.mlb.com/leaderboard"

    def __init__(self, year):
        self.year = year
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        self._expected = None   # player_id -> {est_woba, ...}
        self._statcast = None   # player_id -> {brl_percent, ev95percent, ...}
        self._arsenal = None    # player_id -> [{pitch_name, pitch_usage, ...}, ...]

    def _fetch_csv(self, url, params):
        """Fetch CSV from Savant, return list of dicts."""
        import csv, io
        try:
            r = self.session.get(url, params=params, timeout=30)
            r.raise_for_status()
            text = r.text.lstrip('\ufeff')  # strip BOM
            reader = csv.DictReader(io.StringIO(text))
            return list(reader)
        except Exception as e:
            print(f"  [Savant ERROR] {url}: {e}")
            return []

    def _load_expected(self):
        """Load xwOBA leaderboard (est_woba column)."""
        if self._expected is not None:
            return
        rows = self._fetch_csv(f"{self.BASE}/expected_statistics", {
            'type': 'pitcher', 'year': self.year, 'min': 1, 'csv': 'true',
        })
        self._expected = {}
        for r in rows:
            pid = r.get('player_id', '').strip().strip('"')
            if pid:
                self._expected[int(pid)] = {
                    'xwoba': safe_float(r.get('est_woba')),
                    'woba': safe_float(r.get('woba')),
                }
        print(f"  [Savant] Expected stats loaded: {len(self._expected)} pitchers (xwOBA)")

    def _load_statcast(self):
        """Load barrel% and hard-hit% leaderboard."""
        if self._statcast is not None:
            return
        rows = self._fetch_csv(f"{self.BASE}/statcast", {
            'type': 'pitcher', 'year': self.year, 'min': 1, 'csv': 'true',
        })
        self._statcast = {}
        for r in rows:
            pid = r.get('player_id', '').strip().strip('"')
            if pid:
                self._statcast[int(pid)] = {
                    'barrel_pct': safe_float(r.get('brl_percent')),
                    'hard_hit_pct': safe_float(r.get('ev95percent')),
                }
        print(f"  [Savant] Statcast loaded: {len(self._statcast)} pitchers (barrel/hard-hit)")

    def _load_arsenal(self):
        """Load pitch arsenal (pitch type usage per pitcher)."""
        if self._arsenal is not None:
            return
        rows = self._fetch_csv(f"{self.BASE}/pitch-arsenal-stats", {
            'type': 'pitcher', 'year': self.year, 'min': 10, 'csv': 'true',
        })
        self._arsenal = {}
        for r in rows:
            pid = r.get('player_id', '').strip().strip('"')
            if not pid:
                continue
            pid = int(pid)
            if pid not in self._arsenal:
                self._arsenal[pid] = []
            usage = safe_float(r.get('pitch_usage'))
            self._arsenal[pid].append({
                'pitch': (r.get('pitch_name') or r.get('pitch_type', '?')).strip(),
                'usage': usage,
            })
        # Sort each pitcher's pitches by usage descending
        for pid in self._arsenal:
            self._arsenal[pid].sort(key=lambda x: x['usage'] or 0, reverse=True)
        print(f"  [Savant] Pitch arsenal loaded: {len(self._arsenal)} pitchers")

    def load_all(self):
        """Load all 3 Savant leaderboards. Call once per session."""
        self._load_expected()
        self._load_statcast()
        self._load_arsenal()

    def get_sp_statcast(self, pitcher_id):
        """Get Statcast data for a specific pitcher.
        Returns dict: xwoba, barrel_pct, hard_hit_pct, pitch_mix (str).
        """
        if not pitcher_id:
            return {'xwoba': None, 'barrel_pct': None, 'hard_hit_pct': None, 'pitch_mix': None}

        xw = (self._expected or {}).get(pitcher_id, {})
        sc = (self._statcast or {}).get(pitcher_id, {})
        ar = (self._arsenal or {}).get(pitcher_id, [])

        # Format pitch mix as "FF:45/SL:28/CH:15" (top 3)
        pitch_mix = None
        if ar:
            parts = []
            for p in ar[:3]:
                name = p['pitch']
                pct = round(p['usage'] * 100) if p['usage'] and p['usage'] <= 1 else p['usage']
                if pct:
                    parts.append(f"{name}:{pct:.0f}" if isinstance(pct, float) else f"{name}:{pct}")
            pitch_mix = '/'.join(parts) if parts else None

        return {
            'xwoba': xw.get('xwoba'),
            'barrel_pct': sc.get('barrel_pct'),
            'hard_hit_pct': sc.get('hard_hit_pct'),
            'pitch_mix': pitch_mix,
        }


# ============================================================
# WEATHER API (Open-Meteo)
# ============================================================

def get_weather_forecast(home_abbr, date_str):
    """Get weather forecast for a game using home team's stadium coordinates."""
    coords = STADIUM_COORDINATES.get(home_abbr)
    if not coords:
        return {'Avg_Temp_C': None, 'Precipitation_mm': None, 'Max_Wind_kph': None}

    try:
        r = requests.get("https://api.open-meteo.com/v1/forecast", params={
            'latitude': coords['lat'],
            'longitude': coords['lon'],
            'start_date': date_str,
            'end_date': date_str,
            'daily': 'temperature_2m_mean,precipitation_sum,wind_speed_10m_max,wind_direction_10m_dominant',
            'timezone': 'auto'
        }, timeout=15)
        r.raise_for_status()
        daily = r.json().get('daily', {})
        return {
            'Avg_Temp_C': daily.get('temperature_2m_mean', [None])[0],
            'Precipitation_mm': daily.get('precipitation_sum', [None])[0],
            'Max_Wind_kph': daily.get('wind_speed_10m_max', [None])[0],
            'Wind_Dir_Deg': daily.get('wind_direction_10m_dominant', [None])[0],
        }
    except requests.exceptions.RequestException as e:
        print(f"  [Weather ERROR] {home_abbr}: {e}")
        return {'Avg_Temp_C': None, 'Precipitation_mm': None, 'Max_Wind_kph': None, 'Wind_Dir_Deg': None}


# ============================================================
# O/U LINE (The Odds API)
# ============================================================

def get_ou_lines():
    """
    Get O/U totals lines from The Odds API.
    Returns dict: {('HOME_ABBR', 'AWAY_ABBR'): line, ...}
    Uses first available bookmaker's line (typically FanDuel/DraftKings).
    """
    try:
        r = requests.get('https://api.the-odds-api.com/v4/sports/baseball_mlb/odds', params={
            'apiKey': ODDS_API_KEY,
            'regions': 'us',
            'markets': 'totals',
            'oddsFormat': 'american',
        }, timeout=15)
        r.raise_for_status()
        data = r.json()
        remaining = r.headers.get('x-requests-remaining', '?')
        print(f"  [Odds API] {len(data)} games loaded (requests remaining: {remaining})")
    except requests.exceptions.RequestException as e:
        print(f"  [Odds API ERROR] {e}")
        return {}

    result = {}
    for game in data:
        home = ODDS_NAME_TO_ABBR.get(game.get('home_team'))
        away = ODDS_NAME_TO_ABBR.get(game.get('away_team'))
        if not home or not away:
            continue

        # Get O/U line from first bookmaker that has it
        ou_line = None
        for bm in game.get('bookmakers', []):
            for mkt in bm.get('markets', []):
                if mkt['key'] == 'totals':
                    for outcome in mkt.get('outcomes', []):
                        if outcome['name'] == 'Over' and outcome.get('point'):
                            ou_line = outcome['point']
                            break
                if ou_line:
                    break
            if ou_line:
                break

        if ou_line:
            result[(home, away)] = ou_line

    return result


# ============================================================
# DATA CALCULATIONS
# ============================================================

def calc_l5g(schedule, team_id):
    """Calculate Last 5 Games stats from schedule data."""
    completed = []
    for g in schedule:
        if g['status'] != 'Final':
            continue
        is_home = g['home_id'] == team_id
        runs_for = g['home_score'] if is_home else g['away_score']
        runs_against = g['away_score'] if is_home else g['home_score']
        completed.append({
            'win': runs_for > runs_against,
            'runs_for': runs_for,
            'runs_against': runs_against,
        })

    last5 = completed[-5:]
    if not last5:
        return {'wp': None, 'avg_runs': None, 'avg_runs_against': None}

    n = len(last5)
    return {
        'wp': round(sum(1 for g in last5 if g['win']) / n, 3),
        'avg_runs': round(sum(g['runs_for'] for g in last5) / n, 2),
        'avg_runs_against': round(sum(g['runs_against'] for g in last5) / n, 2),
    }


def calc_h2h(schedule, team_id, opponent_id):
    """Calculate recent Head-to-Head win percentage."""
    h2h_games = []
    for g in schedule:
        if g['status'] != 'Final':
            continue
        other_id = g['away_id'] if g['home_id'] == team_id else g['home_id']
        if other_id != opponent_id:
            continue
        is_home = g['home_id'] == team_id
        runs_for = g['home_score'] if is_home else g['away_score']
        runs_against = g['away_score'] if is_home else g['home_score']
        h2h_games.append(runs_for > runs_against)

    if not h2h_games:
        return None
    return round(sum(h2h_games) / len(h2h_games), 3)


def calc_home_away_runs(schedule, team_id, location='home'):
    """Calculate avg runs scored/allowed in home or away games only."""
    games = []
    for g in schedule:
        if g['status'] != 'Final':
            continue
        is_home = g['home_id'] == team_id
        if location == 'home' and not is_home:
            continue
        if location == 'away' and is_home:
            continue
        runs_for = g['home_score'] if is_home else g['away_score']
        runs_against = g['away_score'] if is_home else g['home_score']
        games.append({'rf': runs_for, 'ra': runs_against})

    if not games:
        return {'avg_scored': None, 'avg_allowed': None}

    n = len(games)
    return {
        'avg_scored': round(sum(g['rf'] for g in games) / n, 2),
        'avg_allowed': round(sum(g['ra'] for g in games) / n, 2),
    }


def calc_series_info(schedule, team_id, opponent_id, game_date):
    """Calculate series game number and wins for each team."""
    # Find consecutive games between these two teams ending on/before game_date
    series_games = []
    all_matchups = []

    for g in schedule:
        other_id = g['away_id'] if g['home_id'] == team_id else g['home_id']
        if other_id == opponent_id:
            all_matchups.append(g)

    # Find the current series (consecutive dates)
    for i, g in enumerate(all_matchups):
        if g['date'] <= game_date:
            # Check if it's part of current series (within 3 days of previous)
            if series_games:
                prev_date = datetime.strptime(series_games[-1]['date'], '%Y-%m-%d')
                curr_date = datetime.strptime(g['date'], '%Y-%m-%d')
                if (curr_date - prev_date).days > 2:
                    series_games = []  # New series
            series_games.append(g)

    game_num = len(series_games)
    home_wins = 0
    away_wins = 0
    for g in series_games:
        if g['status'] != 'Final':
            continue
        if g['home_score'] > g['away_score']:
            home_wins += 1
        elif g['away_score'] > g['home_score']:
            away_wins += 1

    return {
        'game_num': game_num if game_num > 0 else 1,
        'home_wins': home_wins,
        'away_wins': away_wins,
    }


def calc_pm(sp_eras):
    """
    Calculate PM (Pitching Metric) = MAD of SP ERA splits.
    sp_eras: dict with keys: season, home, away, day, night, last7d, vl, vr
    Only uses values that are not None. Needs at least 2 values.
    """
    values = [v for v in sp_eras.values() if v is not None]
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    mad = sum(abs(v - mean) for v in values) / len(values)
    return round(mad, 2)


def calc_bm(bat_avgs):
    """
    Calculate BM (Batting Metric) = AVG of absolute deviations from season AVG x 100.
    bat_avgs: dict with keys: season, home, away, vl, vr, day, night, last7d
    Season is the baseline. Only uses values that are not None. Needs season + at least 1 other.
    """
    baseline = bat_avgs.get('season')
    if baseline is None:
        return None
    others = {k: v for k, v in bat_avgs.items() if k != 'season' and v is not None}
    if not others:
        return None
    deviations = [abs(v - baseline) for v in others.values()]
    bm = (sum(deviations) / len(deviations)) * 100
    return round(bm, 2)


def parse_ip(ip_str):
    """Parse innings pitched string (e.g. '5.2' = 5 2/3 innings)."""
    if not ip_str:
        return 0.0
    try:
        parts = str(ip_str).split('.')
        full = int(parts[0])
        frac = int(parts[1]) / 3 if len(parts) > 1 else 0.0
        return full + frac
    except (ValueError, IndexError):
        return 0.0


def calc_bullpen_stats(schedule, team_id, mlb_api, days_era=30, days_ip=3):
    """
    Calculate bullpen stats from game boxscores.
    - BP_ERA_30d: Bullpen ERA over last 30 days
    - BP_IRS_30d: Bullpen Inherited Runners Scored % over last 30 days
    - BP_IP_3d: Bullpen innings pitched in last 3 days
    - BP_WORKLOAD: Bullpen appearances in last 7 days
    """
    today = datetime.now()

    # Filter completed games within windows
    games_30d = []
    games_7d = []
    games_3d = []

    for g in schedule:
        if g['status'] != 'Final':
            continue
        try:
            game_date = datetime.strptime(g['date'], '%Y-%m-%d')
        except ValueError:
            continue
        diff = (today - game_date).days
        if diff <= days_era:
            games_30d.append(g)
        if diff <= 7:
            games_7d.append(g)
        if diff <= days_ip:
            games_3d.append(g)

    # Collect bullpen stats from boxscores
    total_er = 0
    total_ip = 0.0
    total_ir = 0
    total_irs = 0
    ip_3d = 0.0
    appearances_7d = 0

    def extract_bullpen(game, team_id):
        """Extract bullpen pitcher stats from a single game boxscore."""
        box = mlb_api.get_boxscore(game['game_pk'])
        if not box:
            return []

        # Determine if team is home or away
        side = 'home' if game['home_id'] == team_id else 'away'
        team_data = box.get('teams', {}).get(side, {})
        pitcher_ids = team_data.get('pitchers', [])
        players = team_data.get('players', {})

        relievers = []
        for i, pid in enumerate(pitcher_ids):
            if i == 0:
                continue  # Skip starter (first pitcher)
            key = f'ID{pid}'
            p = players.get(key, {})
            stats = p.get('stats', {}).get('pitching', {})
            if stats:
                relievers.append({
                    'ip': parse_ip(stats.get('inningsPitched', '0')),
                    'er': stats.get('earnedRuns', 0) or 0,
                    'ir': stats.get('inheritedRunners', 0) or 0,
                    'irs': stats.get('inheritedRunnersScored', 0) or 0,
                })
        return relievers

    # 30-day window: ERA + IRS
    print(f"      Fetching {len(games_30d)} boxscores (30d)...", end='', flush=True)
    for g in games_30d:
        relievers = extract_bullpen(g, team_id)
        for r in relievers:
            total_er += r['er']
            total_ip += r['ip']
            total_ir += r['ir']
            total_irs += r['irs']
    print(" done")

    # 3-day window: IP (only need games not already fetched - but boxscores are cached)
    for g in games_3d:
        relievers = extract_bullpen(g, team_id)
        for r in relievers:
            ip_3d += r['ip']

    # 7-day window: appearances
    for g in games_7d:
        relievers = extract_bullpen(g, team_id)
        appearances_7d += len(relievers)

    # Calculate final stats
    bp_era = round((total_er / total_ip) * 9, 2) if total_ip > 0 else None
    bp_irs = round(total_irs / total_ir * 100, 1) if total_ir > 0 else None
    bp_ip_3d = round(ip_3d, 1)
    bp_workload = appearances_7d

    return {
        'bp_era_30d': bp_era,
        'bp_irs_30d': bp_irs,
        'bp_ip_3d': bp_ip_3d,
        'bp_workload': bp_workload,
    }


def calc_bullpen_fatigue(schedule, team_id, mlb_api, game_date):
    """Track per-pitcher bullpen fatigue over last 7 days.
    Returns: {top_relievers: [{name, id, last_date, last_ip, total_ip_7d, apps_7d}, ...]}
    Uses cached boxscores from calc_bullpen_stats.
    """
    today = datetime.strptime(game_date, '%Y-%m-%d')
    cutoff = today - timedelta(days=7)

    reliever_log = {}  # pid -> {'name', 'appearances': [{'date','ip'}]}

    for g in schedule:
        if g['status'] != 'Final':
            continue
        try:
            gd = datetime.strptime(g['date'], '%Y-%m-%d')
        except ValueError:
            continue
        if gd < cutoff or gd >= today:
            continue

        box = mlb_api.get_boxscore(g['game_pk'])
        if not box:
            continue

        side = 'home' if g['home_id'] == team_id else 'away'
        team_data = box.get('teams', {}).get(side, {})
        pitcher_ids = team_data.get('pitchers', [])
        players = team_data.get('players', {})

        for i, pid in enumerate(pitcher_ids):
            if i == 0:
                continue  # skip starter
            key = f'ID{pid}'
            p = players.get(key, {})
            stats = p.get('stats', {}).get('pitching', {})
            if not stats:
                continue
            name = p.get('person', {}).get('fullName', f'#{pid}')
            ip = parse_ip(stats.get('inningsPitched', '0'))
            if pid not in reliever_log:
                reliever_log[pid] = {'name': name, 'appearances': []}
            reliever_log[pid]['appearances'].append({
                'date': g['date'], 'ip': round(ip, 1),
            })

    # Build top 5 by appearances (high-leverage proxy)
    top = sorted(reliever_log.items(), key=lambda x: len(x[1]['appearances']), reverse=True)[:5]

    result = []
    three_days_ago = (today - timedelta(days=3)).strftime('%Y-%m-%d')
    for pid, info in top:
        apps = info['appearances']
        last = apps[-1] if apps else None
        apps_3d = [a for a in apps if a['date'] >= three_days_ago]
        result.append({
            'id': pid,
            'name': info['name'],
            'apps_7d': len(apps),
            'last_appearance': last['date'] if last else None,
            'last_ip': last['ip'] if last else None,
            'ip_3d': round(sum(a['ip'] for a in apps_3d), 1),
            'apps_3d': len(apps_3d),
        })
    return result


def safe_float(val, default=None):
    """Safely convert to float."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ============================================================
# LINEUP-BASED CALCULATIONS
# ============================================================

def _normalize_name(s):
    """Strip accents and lowercase for fuzzy name matching."""
    import unicodedata
    if not s:
        return ''
    nfkd = unicodedata.normalize('NFKD', s)
    return ''.join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def scrape_rotowire_lineups():
    """Scrape today's expected/confirmed lineups from RotoWire.
    Returns flat dict: {GAME_ID: {home_roster_batters: [...], away_roster_batters: [...]}}
    Batter IDs are None — must run resolve_lineup_ids() after.
    """
    import re
    url = 'https://www.rotowire.com/baseball/daily-lineups.php'
    ROTOWIRE_TO_STD = {
        'WSH': 'WSN', 'KC': 'KCR', 'SD': 'SDP', 'TB': 'TBR',
        'SF': 'SFG', 'AZ': 'ARI', 'ATH': 'OAK',
    }
    try:
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        r.raise_for_status()
        html = r.text
    except requests.exceptions.RequestException as e:
        print(f"  [RotoWire ERROR] {e}")
        return {}

    games_raw = re.split(r'<div class="lineup is-mlb', html)[1:]
    if not games_raw:
        print("  [RotoWire] No games found on page")
        return {}

    def _parse_side(game_html, side_class):
        pattern = f'lineup__list {side_class}'
        m = re.search(pattern + r'(.*?)(?=lineup__list|lineup__bottom|\Z)', game_html, re.DOTALL)
        if not m:
            return [], None
        sec = m.group(1)
        sp_m = re.search(r'highlight-name.*?<a[^>]*>(.*?)</a>', sec, re.DOTALL)
        sp = sp_m.group(1).strip() if sp_m else None
        batters = []
        names = [pm.group(1) for pm in re.finditer(r'<a title="([^"]+)" href="/baseball/player', sec)]
        pos_list = re.findall(r'lineup__pos">(.*?)</div>', sec)
        bats_list = re.findall(r'lineup__bats">(.*?)</span>', sec)
        for i, name in enumerate(names[:9]):
            batters.append({
                'id': None,
                'name': name,
                'position': pos_list[i] if i < len(pos_list) else '',
                'bats': bats_list[i] if i < len(bats_list) else '',
                'order': i + 1,
            })
        return batters, sp

    results = {}
    for g in games_raw:
        visit_m = re.search(r'is-visit.*?lineup__abbr">(.*?)</div>', g, re.DOTALL)
        home_m = re.search(r'is-home.*?lineup__abbr">(.*?)</div>', g, re.DOTALL)
        if not visit_m or not home_m:
            continue
        away = ROTOWIRE_TO_STD.get(visit_m.group(1).strip(), visit_m.group(1).strip())
        home = ROTOWIRE_TO_STD.get(home_m.group(1).strip(), home_m.group(1).strip())

        away_lineup, away_sp = _parse_side(g, 'is-visit')
        home_lineup, home_sp = _parse_side(g, 'is-home')

        gid = f"{home}_{away}"
        results[gid] = {
            'home_roster_batters': home_lineup,
            'away_roster_batters': away_lineup,
        }

    print(f"  [RotoWire] Scraped {len(results)} games with lineups")
    return results


def load_lineups(date_str):
    """Load daily lineups from SQLite DB (data/mlb_season.db → lineups table).

    Returns flat structure: {GAME_ID: {home_roster_batters: [...], away_roster_batters: [...]}}.
    Batters have id=None initially; resolve_lineup_ids() must run after to fill them.

    Fallback: daily_lineups.json if DB is empty.
    """
    # 1차: SQLite DB
    try:
        from lineup_db import load_lineups_from_db
        flat = load_lineups_from_db(date_str)
        if flat:
            empty = sum(1 for g in flat.values()
                        if not g.get('home_roster_batters') and not g.get('away_roster_batters'))
            print(f"  Loaded lineups from DB: {len(flat)} games, {empty} empty")
            return flat
    except Exception as e:
        print(f"  DB lineup load error: {e}")

    # 2차 폴백: 기존 JSON
    import os
    path = r'C:\Users\AA\MLB\daily_lineups.json'
    if not os.path.exists(path):
        print(f"  No lineup data found (DB empty, JSON missing)")
        return {}

    print(f"  [FALLBACK] Loading from {path}")
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"  Lineup load error: {e}")
        return {}

    file_date = data.get('date')
    if file_date and file_date != date_str:
        print(f"  [WARN] Lineup file date={file_date} but requested {date_str} — IGNORING stale file")
        return {}

    games_obj = data.get('games', {})
    if isinstance(games_obj, list):
        games_iter = [(g.get('game_id'), g) for g in games_obj]
    elif isinstance(games_obj, dict):
        games_iter = list(games_obj.items())
    else:
        return {}

    def _convert(lineup_list):
        out = []
        for entry in (lineup_list or []):
            out.append({
                'id': entry.get('id'),
                'name': entry.get('name'),
                'position': entry.get('position'),
                'order': entry.get('order'),
                'bats': entry.get('bats'),
            })
        return out

    flat = {}
    empty = 0
    for gid, g in games_iter:
        if not gid or not isinstance(g, dict):
            continue
        home_block = g.get('home', {}) or {}
        away_block = g.get('away', {}) or {}
        home_lineup = _convert(home_block.get('lineup'))
        away_lineup = _convert(away_block.get('lineup'))
        if not home_lineup and not away_lineup:
            empty += 1
        flat[gid] = {
            'home_roster_batters': home_lineup,
            'away_roster_batters': away_lineup,
        }

    print(f"  [FALLBACK] Loaded lineups: {path} ({len(flat)} games, {empty} empty)")
    return flat


def resolve_lineup_ids(lineup_games, mlb_api):
    """Fill missing batter IDs by matching names against MLB Stats API season roster.

    Mutates lineup_games in place. Uses accent-insensitive name matching.
    """
    if not lineup_games:
        return

    players = mlb_api.get_all_players()
    if not players:
        print("  [Resolver] No player roster available — skipping ID resolution")
        return

    name_map = {}
    for p in players:
        full = p.get('fullName')
        if not full:
            continue
        key = _normalize_name(full)
        name_map[key] = p['id']

    print(f"  [Resolver] Built name->id map: {len(name_map)} players")

    resolved = 0
    missed = 0
    for gid, g in lineup_games.items():
        for side in ('home_roster_batters', 'away_roster_batters'):
            for b in g.get(side, []):
                if b.get('id'):
                    continue
                key = _normalize_name(b.get('name', ''))
                pid = name_map.get(key)
                if pid:
                    b['id'] = pid
                    resolved += 1
                else:
                    missed += 1

    print(f"  [Resolver] Resolved {resolved} batter IDs, {missed} unmatched")


def _lineup_game_id_variants_in_set(gid, today_gids):
    """Check if any abbreviation variant of `gid` is in today_gids.
    Handles abbreviation mismatches (WSH↔WSN, SD↔SDP, TB↔TBR, KC↔KCR, SF↔SFG, AZ↔ARI)."""
    if '_' not in gid:
        return False
    h, a = gid.split('_', 1)
    for hv in _lineup_game_id_variants(h, a):
        if hv in today_gids:
            return True
    return False


def _lineup_game_id_variants(home_abbr, away_abbr):
    """Generate possible GAME_ID variants for lineup JSON lookup.
    Lineup JSON may use different abbreviations (WSH vs WSN, SD vs SDP, etc.)."""
    # Standard abbreviation -> lineup JSON abbreviation mapping
    ALT = {
        'WSN': 'WSH', 'SDP': 'SD', 'TBR': 'TB', 'KCR': 'KC',
        'SFG': 'SF', 'ARI': 'AZ',
        # Reverse mappings too
        'WSH': 'WSN', 'SD': 'SDP', 'TB': 'TBR', 'KC': 'KCR',
        'SF': 'SFG', 'AZ': 'ARI',
    }
    homes = [home_abbr] + ([ALT[home_abbr]] if home_abbr in ALT else [])
    aways = [away_abbr] + ([ALT[away_abbr]] if away_abbr in ALT else [])
    variants = []
    for h in homes:
        for a in aways:
            variants.append(f"{h}_{a}")
    return variants


def get_lineup_batters(lineup_games, home_abbr, away_abbr, side):
    """
    Get batter list from lineup JSON for a specific team.
    side: 'home' or 'away'
    Returns list of {'id': int, 'name': str, 'position': str} or None.
    """
    # Try all abbreviation variants
    game_data = None
    for gid in _lineup_game_id_variants(home_abbr, away_abbr):
        game_data = lineup_games.get(gid)
        if game_data:
            break
        # Try with _G2 suffix etc
        for key, val in lineup_games.items():
            if key.startswith(gid):
                game_data = val
                break
        if game_data:
            break
    if not game_data:
        return None

    key = f"{side}_roster_batters"
    batters = game_data.get(key, [])
    if not batters:
        return None
    return batters


def calc_lineup_batting(mlb_api, batters, sit_code=None, last7d=False):
    """
    Calculate average batting AVG from individual batter stats.
    Returns float average or None if no data.
    """
    avgs = []
    for b in batters:
        bid = b.get('id')
        if not bid:
            continue
        if last7d:
            stats = mlb_api.get_batter_stats_last7d(bid)
        else:
            stats = mlb_api.get_batter_stats(bid, sit_code=sit_code)
        avg = safe_float(stats.get('avg'))
        if avg is not None:
            avgs.append(avg)
    if not avgs:
        return None
    return round(sum(avgs) / len(avgs), 3)


def calc_lineup_obp(mlb_api, batters):
    """Calculate average OBP from individual batter season stats."""
    obps = []
    for b in batters:
        bid = b.get('id')
        if not bid:
            continue
        stats = mlb_api.get_batter_stats(bid)
        obp = safe_float(stats.get('obp'))
        if obp is not None:
            obps.append(obp)
    if not obps:
        return None
    return round(sum(obps) / len(obps), 3)


def calc_lineup_bvp(mlb_api, batters, pitcher_id):
    """
    Calculate BvP stats for lineup batters vs opposing SP.
    Returns dict: bvp_ab, bvp_avg, bvp_obp, bvp_conf
    bvp_conf = number of batters with 5+ AB (confidence indicator)
    """
    total_ab = 0
    total_hits = 0
    total_bb = 0
    total_hbp = 0
    total_pa = 0
    batters_with_data = 0

    for b in batters:
        bid = b.get('id')
        if not bid or not pitcher_id:
            continue
        bvp = mlb_api.get_bvp(bid, pitcher_id)
        ab = bvp.get('atBats', 0)
        if ab > 0:
            total_ab += ab
            total_hits += bvp.get('hits', 0)
            total_bb += bvp.get('baseOnBalls', 0)
            total_hbp += bvp.get('hitByPitch', 0)
            total_pa += bvp.get('plateAppearances', ab)
            if ab >= 5:
                batters_with_data += 1

    if total_ab == 0:
        return {'bvp_ab': 0, 'bvp_avg': None, 'bvp_obp': None, 'bvp_conf': 0}

    bvp_avg = round(total_hits / total_ab, 3)
    bvp_obp = round((total_hits + total_bb + total_hbp) / total_pa, 3) if total_pa > 0 else None

    return {
        'bvp_ab': total_ab,
        'bvp_avg': bvp_avg,
        'bvp_obp': bvp_obp,
        'bvp_conf': batters_with_data,
    }


# ============================================================
# TRAVEL / FATIGUE
# ============================================================

def calc_travel_fatigue(schedule, team_id, today_venue_abbr, game_date):
    """Calculate travel distance, rest days, and road streak for a team.

    Returns dict: travel_miles, rest_days, road_streak
    - travel_miles: haversine distance from last game location to today's (miles)
    - rest_days: days since last completed game (0 = back-to-back)
    - road_streak: consecutive road games INCLUDING today (0 if home today)
    """
    today = datetime.strptime(game_date, '%Y-%m-%d')

    # Collect completed games before today, sorted by date
    past = []
    for g in schedule:
        if g['status'] != 'Final':
            continue
        try:
            gd = datetime.strptime(g['date'], '%Y-%m-%d')
        except ValueError:
            continue
        if gd < today:
            past.append(g)
    past.sort(key=lambda g: g['date'])

    if not past:
        return {'travel_miles': None, 'rest_days': None, 'road_streak': None}

    last_game = past[-1]
    last_date = datetime.strptime(last_game['date'], '%Y-%m-%d')
    rest_days = (today - last_date).days - 1  # 0 = back-to-back

    # Where was the last game played? (home team's stadium)
    last_home_id = last_game['home_id']
    last_venue = MLB_ID_TO_ABBR.get(last_home_id, '')

    # Distance from last venue to today's venue
    c1 = STADIUM_COORDINATES.get(last_venue)
    c2 = STADIUM_COORDINATES.get(today_venue_abbr)
    if c1 and c2 and last_venue != today_venue_abbr:
        travel_miles = round(haversine_miles(c1['lat'], c1['lon'], c2['lat'], c2['lon']))
    else:
        travel_miles = 0

    # Road streak: count consecutive games where team was away, going backwards
    road_streak = 0
    is_home_today = (MLB_TEAM_IDS.get(today_venue_abbr) == team_id)
    if not is_home_today:
        road_streak = 1  # today counts
        for g in reversed(past):
            if g['home_id'] != team_id:
                road_streak += 1
            else:
                break
    # If home today, road_streak = 0

    return {
        'travel_miles': travel_miles,
        'rest_days': max(rest_days, 0),
        'road_streak': road_streak,
    }


# ============================================================
# SITUATIONAL TAGS
# ============================================================

def calc_situational_tag(schedule, team_id, opponent_id, game_date, standings,
                         tomorrow_schedule=None):
    """Determine situational tag for a team: letdown, revenge, sandwich, or none.

    - letdown: won yesterday vs strong team (>55% WP), today face weak team (<45% WP)
    - revenge: lost to TODAY's opponent within last 10 days
    - sandwich: tomorrow face strong team (>55% WP), today face weak team (<45% WP)
    """
    today = datetime.strptime(game_date, '%Y-%m-%d')
    yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    ten_days_ago = (today - timedelta(days=10)).strftime('%Y-%m-%d')

    # Helper: get team win_pct from standings
    def wp(tid):
        return standings.get(tid, {}).get('win_pct', 0.5)

    opponent_wp = wp(opponent_id)

    # --- REVENGE: lost to today's opponent in a PREVIOUS series (not current series) ---
    # Skip games within 2 days (likely same series) — only flag if lost in an earlier series
    three_days_ago = (today - timedelta(days=3)).strftime('%Y-%m-%d')
    for g in reversed(schedule):
        if g['status'] != 'Final':
            continue
        if g['date'] < ten_days_ago:
            break
        if g['date'] >= three_days_ago:
            continue  # skip current series games
        other_id = g['away_id'] if g['home_id'] == team_id else g['home_id']
        if other_id != opponent_id:
            continue
        is_home = g['home_id'] == team_id
        my_runs = g['home_score'] if is_home else g['away_score']
        their_runs = g['away_score'] if is_home else g['home_score']
        if my_runs < their_runs:
            return 'revenge'

    # --- LETDOWN: won yesterday vs strong team, today face weak team ---
    for g in reversed(schedule):
        if g['date'] != yesterday:
            continue
        if g['status'] != 'Final':
            continue
        is_home = g['home_id'] == team_id
        my_runs = g['home_score'] if is_home else g['away_score']
        their_runs = g['away_score'] if is_home else g['home_score']
        yesterday_opp = g['away_id'] if is_home else g['home_id']
        if my_runs > their_runs and wp(yesterday_opp) > 0.550 and opponent_wp < 0.450:
            return 'letdown'
        break

    # --- SANDWICH: tomorrow face strong team, today face weak team ---
    if tomorrow_schedule:
        tomorrow_str = (today + timedelta(days=1)).strftime('%Y-%m-%d')
        for g in tomorrow_schedule:
            if g['date'] != tomorrow_str:
                continue
            if g['home_id'] == team_id or g['away_id'] == team_id:
                tmrw_opp = g['away_id'] if g['home_id'] == team_id else g['home_id']
                if wp(tmrw_opp) > 0.550 and opponent_wp < 0.450:
                    return 'sandwich'
                break

    return 'none'


# ============================================================
# MAIN COLLECTOR
# ============================================================

class MLBKing:
    """84-column game data collector."""

    def __init__(self, year):
        self.year = year
        self.mlb = MLBApi(year)
        self.fg = FanGraphsApi()
        self.savant = SavantApi(year)
        self._today_games_cache = None

    def collect_all(self, date_str):
        """Collect all 84 columns for all games on a given date."""
        print(f"\n{'='*60}")
        print(f"  MLB KING v2 - Collecting data for {date_str}")
        print(f"{'='*60}\n")

        # Step 1: Get today's games
        games = self.mlb.get_today_games(date_str)
        self._today_games_cache = games
        if not games:
            print("No games found for this date.")
            return pd.DataFrame(columns=COLUMNS)

        print(f"Found {len(games)} games\n")

        # Step 2: Load standings (1 API call)
        print("[1/5] Loading standings...")
        standings = self.mlb.get_standings()

        # Step 3: Load FanGraphs data (3 API calls)
        print("[2/6] Loading FanGraphs data (wOBA, SIERA, Park Factors)...")
        fg_batting = self.fg.get_team_batting(self.year)
        fg_pitching = self.fg.get_team_pitching(self.year)
        park_factors = self.fg.get_park_factors(self.year)

        # Step 3.5: Load Baseball Savant Statcast (3 CSV downloads)
        print("[3/7] Loading Baseball Savant (xwOBA, barrel%, pitch mix)...")
        self.savant.load_all()

        # Step 4: Load O/U lines (1 API call)
        print("[4/7] Loading O/U lines...")
        ou_lines = get_ou_lines()

        # Step 4.5: Load lineups (local JSON → RotoWire fallback) + resolve IDs
        print("[4/7] Loading lineup data...")
        lineup_games = load_lineups(date_str)

        # Coverage check: how many of TODAY'S games have actual batters?
        total_games = len(games)
        covered = 0
        if lineup_games:
            today_gids = {f"{g['home_abbr']}_{g['away_abbr']}" for g in games}
            for gid, lg in lineup_games.items():
                home_ok = len(lg.get('home_roster_batters', [])) >= 8
                away_ok = len(lg.get('away_roster_batters', [])) >= 8
                if (gid in today_gids or _lineup_game_id_variants_in_set(gid, today_gids)) and home_ok and away_ok:
                    covered += 1

        coverage = covered / total_games if total_games else 0
        print(f"  [Lineup] Coverage: {covered}/{total_games} games ({coverage*100:.0f}%) with full lineups")

        # Fallback to RotoWire if coverage < 50%
        if coverage < 0.5:
            print(f"  [Lineup] Coverage too low — falling back to RotoWire scrape...")
            rw_lineups = scrape_rotowire_lineups()
            if rw_lineups:
                lineup_games = rw_lineups
                rw_covered = sum(
                    1 for gid in lineup_games
                    if len(lineup_games[gid].get('home_roster_batters', [])) >= 8
                    and len(lineup_games[gid].get('away_roster_batters', [])) >= 8
                )
                print(f"  [RotoWire] New coverage: {rw_covered}/{total_games} games")

        if lineup_games:
            resolve_lineup_ids(lineup_games, self.mlb)

        # Step 5: Collect per-game data
        print(f"\n[5/7] Collecting team stats for {len(games)} games...")
        rows = []
        for i, game in enumerate(games, 1):
            home_abbr = game['home_abbr']
            away_abbr = game['away_abbr']
            print(f"\n  --- Game {i}/{len(games)}: {away_abbr} @ {home_abbr} ---")

            row = self._collect_game_row(game, standings, fg_batting, fg_pitching, park_factors, ou_lines, lineup_games)
            rows.append(row)

        # Step 6: Build DataFrame
        print(f"\n[6/7] Building DataFrame...")
        df = pd.DataFrame(rows, columns=COLUMNS)

        print(f"\n[7/7] Done! {len(df)} games collected with {len(COLUMNS)} columns.")
        return df

    def _collect_game_row(self, game, standings, fg_batting, fg_pitching, park_factors, ou_lines, lineup_games=None):
        """Collect one full 84-column row for a single game."""
        home_id = game['home_id']
        away_id = game['away_id']
        home_abbr = game['home_abbr']
        away_abbr = game['away_abbr']
        home_sp_id = game['home_pitcher_id']
        away_sp_id = game['away_pitcher_id']
        date_str = game['date']
        day_night = game.get('day_night', 'night')

        # -- Get lineup batters if available --
        home_batters = get_lineup_batters(lineup_games or {}, home_abbr, away_abbr, 'home')
        away_batters = get_lineup_batters(lineup_games or {}, home_abbr, away_abbr, 'away')

        # -- Schedules (for L5G, H2H, series) --
        home_sched = self.mlb.get_schedule(home_id)
        away_sched = self.mlb.get_schedule(away_id)

        # -- HOME TEAM BLOCK (Col 1-36) --
        home_data = self._collect_team_block(
            team_id=home_id, abbr=home_abbr, sp_id=home_sp_id,
            opponent_id=away_id, opponent_sp_id=away_sp_id,
            location='home', day_night=day_night,
            schedule=home_sched, standings=standings,
            fg_batting=fg_batting, fg_pitching=fg_pitching,
            batters=home_batters
        )

        # -- AWAY TEAM BLOCK (Col 37-72) --
        away_data = self._collect_team_block(
            team_id=away_id, abbr=away_abbr, sp_id=away_sp_id,
            opponent_id=home_id, opponent_sp_id=home_sp_id,
            location='away', day_night=day_night,
            schedule=away_sched, standings=standings,
            fg_batting=fg_batting, fg_pitching=fg_pitching,
            batters=away_batters
        )

        # -- SERIES INFO (Col 73-75) --
        series = calc_series_info(home_sched, home_id, away_id, date_str)

        # -- PARK FACTOR (Col 80) --
        pf = park_factors.get(home_abbr, 100)

        # -- WEATHER (Col 77-79) --
        weather = get_weather_forecast(home_abbr, date_str)
        print(f"    [Weather] {home_abbr}: {weather['Avg_Temp_C']}C, {weather['Precipitation_mm']}mm, {weather['Max_Wind_kph']}kph, wind={weather['Wind_Dir_Deg']}deg")

        # -- O/U LINE (Col 76) --
        ou_line = ou_lines.get((home_abbr, away_abbr))
        print(f"    [O/U] {home_abbr}_{away_abbr}: {ou_line}")

        # -- UMPIRE (Col 85-86) --
        ump_name = game.get('ump_name')
        ump_id = game.get('ump_id')
        ump_rpg = self.mlb.get_umpire_rpg(ump_id) if ump_id else None
        if ump_name:
            print(f"    [Ump] {ump_name}: RPG={ump_rpg}")

        # -- TRAVEL / FATIGUE (Col 87-92) --
        home_travel = calc_travel_fatigue(home_sched, home_id, home_abbr, date_str)
        away_travel = calc_travel_fatigue(away_sched, away_id, home_abbr, date_str)
        print(f"    [Travel] HOME: {home_travel['travel_miles']}mi, rest={home_travel['rest_days']}d, road={home_travel['road_streak']}")
        print(f"    [Travel] AWAY: {away_travel['travel_miles']}mi, rest={away_travel['rest_days']}d, road={away_travel['road_streak']}")

        # -- SITUATIONAL TAGS (Col 93-94) --
        home_sit = calc_situational_tag(home_sched, home_id, away_id, date_str, standings)
        away_sit = calc_situational_tag(away_sched, away_id, home_id, date_str, standings)
        if home_sit != 'none' or away_sit != 'none':
            print(f"    [Situation] HOME={home_sit}, AWAY={away_sit}")

        # -- SP STATCAST (Col 95-102) --
        home_sc = self.savant.get_sp_statcast(home_sp_id)
        away_sc = self.savant.get_sp_statcast(away_sp_id)
        if home_sc['xwoba'] or away_sc['xwoba']:
            print(f"    [Savant] HOME SP: xwOBA={home_sc['xwoba']}, Barrel={home_sc['barrel_pct']}%, Mix={home_sc['pitch_mix']}")
            print(f"    [Savant] AWAY SP: xwOBA={away_sc['xwoba']}, Barrel={away_sc['barrel_pct']}%, Mix={away_sc['pitch_mix']}")

        # -- BUILD ROW (103 columns matching COLUMNS order) --
        row = (
            list(home_data.values()) +   # Col 1-36
            list(away_data.values()) +   # Col 37-72
            [                    # Col 73-98
                series.get('game_num'),          # Series_Game_Num
                series.get('home_wins'),         # Series_Home_Wins
                series.get('away_wins'),         # Series_Away_Wins
                ou_line,                         # U/O_LINE
                weather['Avg_Temp_C'],           # Avg_Temp_C
                weather['Precipitation_mm'],     # Precipitation_mm
                weather['Max_Wind_kph'],         # Max_Wind_kph
                weather['Wind_Dir_Deg'],         # Wind_Dir_Deg
                pf,                              # PARK_FACTOR
                ump_name,                        # UMP_NAME
                ump_rpg,                         # UMP_RPG
                home_travel['travel_miles'],     # HOME_TRAVEL_MI
                home_travel['rest_days'],        # HOME_REST_DAYS
                home_travel['road_streak'],      # HOME_ROAD_STREAK
                away_travel['travel_miles'],     # AWAY_TRAVEL_MI
                away_travel['rest_days'],        # AWAY_REST_DAYS
                away_travel['road_streak'],      # AWAY_ROAD_STREAK
                home_sit,                        # HOME_SIT_TAG
                away_sit,                        # AWAY_SIT_TAG
                home_sc['xwoba'],                # HOME_SP_xwOBA
                home_sc['hard_hit_pct'],         # HOME_SP_HardHit
                home_sc['barrel_pct'],           # HOME_SP_Barrel
                home_sc['pitch_mix'],            # HOME_SP_PitchMix
                away_sc['xwoba'],                # AWAY_SP_xwOBA
                away_sc['hard_hit_pct'],         # AWAY_SP_HardHit
                away_sc['barrel_pct'],           # AWAY_SP_Barrel
                away_sc['pitch_mix'],            # AWAY_SP_PitchMix
                None,                            # U/O_RESULT (filled post-game)
                None,                            # Winning Team (filled post-game)
                date_str,                        # DATE
                f"{home_abbr}_{away_abbr}",      # GAME_ID
            ]
        )
        return row

    def _collect_team_block(self, team_id, abbr, sp_id, opponent_id,
                            location, day_night, schedule, standings,
                            fg_batting, fg_pitching, opponent_sp_id=None,
                            batters=None):
        """Collect 36-column block for one team."""
        use_lineup = batters is not None and len(batters) > 0
        src = f"lineup({len(batters)})" if use_lineup else "team avg"
        print(f"    [{abbr}] Collecting stats... (batting source: {src})")

        # --- Standings ---
        st = standings.get(team_id, {})
        gp = st.get('gp', 1) or 1
        swp = st.get('win_pct', None)
        saps = round(st.get('runs_scored', 0) / gp, 2) if gp > 0 else None
        sapa = round(st.get('runs_allowed', 0) / gp, 2) if gp > 0 else None

        # Home/Away win rate from splits
        loc_key = 'home' if location == 'home' else 'away'
        loc_split = st.get('splits', {}).get(loc_key, {})
        loc_win_rate = loc_split.get('pct', None)

        # --- Home/Away only runs ---
        loc_runs = calc_home_away_runs(schedule, team_id, loc_key)

        # --- L5G ---
        l5g = calc_l5g(schedule, team_id)

        # --- H2H ---
        h2h_wp = calc_h2h(schedule, team_id, opponent_id)

        # --- SP Stats (all splits for PM) ---
        sp_season = self.mlb.get_pitcher_stats(sp_id)
        sp_era = safe_float(sp_season.get('era'))

        sp_loc_code = 'h' if location == 'home' else 'a'
        sp_loc = self.mlb.get_pitcher_stats(sp_id, sit_code=sp_loc_code)
        sp_era_loc = safe_float(sp_loc.get('era'))

        # Both home and away for PM
        sp_home = self.mlb.get_pitcher_stats(sp_id, sit_code='h')
        sp_away = self.mlb.get_pitcher_stats(sp_id, sit_code='a')
        sp_era_home = safe_float(sp_home.get('era'))
        sp_era_away = safe_float(sp_away.get('era'))

        # Day/Night
        sp_day_stats = self.mlb.get_pitcher_stats(sp_id, sit_code='d')
        sp_night_stats = self.mlb.get_pitcher_stats(sp_id, sit_code='n')
        sp_era_day = safe_float(sp_day_stats.get('era'))
        sp_era_night = safe_float(sp_night_stats.get('era'))

        # vs LHP / vs RHP for PM
        sp_vl = self.mlb.get_pitcher_stats(sp_id, sit_code='vl')
        sp_vr = self.mlb.get_pitcher_stats(sp_id, sit_code='vr')
        sp_era_vl = safe_float(sp_vl.get('era'))
        sp_era_vr = safe_float(sp_vr.get('era'))

        # Last 7 days for PM
        sp_last7d = self.mlb.get_pitcher_stats_last7d(sp_id)
        sp_era_last7d = safe_float(sp_last7d.get('era'))

        # --- Team Batting (all splits for BM) ---
        if use_lineup:
            # Lineup-based: individual batter stats averaged
            print(f"    [{abbr}] Using lineup-based batting ({len(batters)} batters)...")
            avg_total = calc_lineup_batting(self.mlb, batters)
            avg_vl = calc_lineup_batting(self.mlb, batters, sit_code='vl')
            avg_vr = calc_lineup_batting(self.mlb, batters, sit_code='vr')
            avg_loc = calc_lineup_batting(self.mlb, batters, sit_code=sp_loc_code)
            avg_day = calc_lineup_batting(self.mlb, batters, sit_code='d')
            avg_night = calc_lineup_batting(self.mlb, batters, sit_code='n')
            avg_home = calc_lineup_batting(self.mlb, batters, sit_code='h')
            avg_away = calc_lineup_batting(self.mlb, batters, sit_code='a')
            avg_last7d = calc_lineup_batting(self.mlb, batters, last7d=True)
            obp = calc_lineup_obp(self.mlb, batters)
        else:
            # Fallback: team average stats
            bat_season = self.mlb.get_team_stats(team_id, 'hitting')
            bat_vl = self.mlb.get_team_stats(team_id, 'hitting', 'vl')
            bat_vr = self.mlb.get_team_stats(team_id, 'hitting', 'vr')
            bat_loc = self.mlb.get_team_stats(team_id, 'hitting', sp_loc_code)
            bat_day = self.mlb.get_team_stats(team_id, 'hitting', 'd')
            bat_night = self.mlb.get_team_stats(team_id, 'hitting', 'n')
            bat_home = self.mlb.get_team_stats(team_id, 'hitting', 'h')
            bat_away = self.mlb.get_team_stats(team_id, 'hitting', 'a')
            bat_last7d = self.mlb.get_team_stats_last7d(team_id, 'hitting')

            avg_total = safe_float(bat_season.get('avg'))
            avg_vl = safe_float(bat_vl.get('avg'))
            avg_vr = safe_float(bat_vr.get('avg'))
            avg_loc = safe_float(bat_loc.get('avg'))
            avg_day = safe_float(bat_day.get('avg'))
            avg_night = safe_float(bat_night.get('avg'))
            avg_home = safe_float(bat_home.get('avg'))
            avg_away = safe_float(bat_away.get('avg'))
            avg_last7d = safe_float(bat_last7d.get('avg'))
            obp = safe_float(bat_season.get('obp'))

        # --- Team Pitching (ERA) ---
        pit_season = self.mlb.get_team_stats(team_id, 'pitching')
        team_era = safe_float(pit_season.get('era'))

        # --- FanGraphs: wOBA, SIERA ---
        fg_bat = fg_batting.get(abbr, {})
        woba = safe_float(fg_bat.get('wOBA'))
        fg_pit = fg_pitching.get(abbr, {})
        siera = safe_float(fg_pit.get('SIERA'))

        # --- Bullpen Stats (from boxscores) ---
        bp = calc_bullpen_stats(schedule, team_id, self.mlb)
        bp_era_30d = bp['bp_era_30d']
        bp_irs_30d = bp['bp_irs_30d']
        bp_ip_3d = bp['bp_ip_3d']
        bp_workload = bp['bp_workload']

        # --- Fielding ---
        fielding = self.mlb.get_team_fielding(team_id)
        fpct = safe_float(fielding.get('fielding'))
        errors = fielding.get('errors', None)

        # --- BvP (Batter vs Pitcher) ---
        if use_lineup and opponent_sp_id:
            print(f"    [{abbr}] Calculating BvP vs SP #{opponent_sp_id}...")
            bvp_data = calc_lineup_bvp(self.mlb, batters, opponent_sp_id)
            bvp_ab = bvp_data['bvp_ab']
            bvp_avg = bvp_data['bvp_avg']
            bvp_obp = bvp_data['bvp_obp']
            bvp_conf = bvp_data['bvp_conf']
        else:
            bvp_ab = None
            bvp_avg = None
            bvp_obp = None
            bvp_conf = None

        # --- PM / BM (split consistency metrics) ---
        pm = calc_pm({
            'season': sp_era,
            'home': sp_era_home,
            'away': sp_era_away,
            'day': sp_era_day,
            'night': sp_era_night,
            'last7d': sp_era_last7d,
            'vl': sp_era_vl,
            'vr': sp_era_vr,
        })
        bm = calc_bm({
            'season': avg_total,
            'home': avg_home,
            'away': avg_away,
            'vl': avg_vl,
            'vr': avg_vr,
            'day': avg_day,
            'night': avg_night,
            'last7d': avg_last7d,
        })

        bvp_str = f"BvP={bvp_ab}AB/{bvp_avg}" if bvp_ab else "BvP=N/A"
        print(f"    [{abbr}] Done. SWP={swp}, ERA_SP={sp_era}, BA={avg_total}, OBP={obp}, {bvp_str}")

        return {
            'swp': swp,
            'saps': saps,
            'sapa': sapa,
            'loc_win_rate': loc_win_rate,
            'saps_loc': loc_runs['avg_scored'],
            'sapa_loc': loc_runs['avg_allowed'],
            'l5g_wp': l5g['wp'],
            'l5g_runs': l5g['avg_runs'],
            'l5g_runs_against': l5g['avg_runs_against'],
            'h2h_wp': h2h_wp,
            'sp_era': sp_era,
            'avg_total': avg_total,
            'avg_vl': avg_vl,
            'avg_vr': avg_vr,
            'avg_loc': avg_loc,
            'sp_era_loc': sp_era_loc,
            'avg_day': avg_day,
            'sp_era_day': sp_era_day,
            'avg_night': avg_night,
            'sp_era_night': sp_era_night,
            'obp': obp,
            'team_era': team_era,
            'pm': pm,
            'bm': bm,
            'woba': woba,
            'siera': siera,
            'bp_era_30d': bp_era_30d,
            'bp_irs_30d': bp_irs_30d,
            'bp_ip_3d': bp_ip_3d,
            'bp_workload': bp_workload,
            'fpct': fpct,
            'errors': errors,
            'bvp_ab': bvp_ab,
            'bvp_avg': bvp_avg,
            'bvp_obp': bvp_obp,
            'bvp_conf': bvp_conf,
        }


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    """Main entry point — collect today's data and save to CSV.
    Usage: python mlb_king.py [YYYY-MM-DD]  (defaults to today EDT)
    """
    if len(sys.argv) > 1:
        today_edt = sys.argv[1]
    else:
        today_edt = (datetime.now(tz=timezone.utc) + timedelta(hours=-4)).strftime('%Y-%m-%d')
    year = int(today_edt[:4])

    print(f"MLB KING v2 — Date: {today_edt}")

    collector = MLBKing(year=year)
    df = collector.collect_all(today_edt)

    if df.empty:
        print("No data collected.")
        return

    # Save to CSV + JSON (atomic write — same-dir temp + os.replace)
    # 1. Write to .tmp in same dir (same filesystem = atomic rename possible)
    # 2. Explicit flush + close before rename
    # 3. CSV: lineterminator='\n' to avoid Windows \r\r\n blank rows
    base_dir = os.path.dirname(os.path.abspath(__file__))

    csv_path = os.path.join(base_dir, 'MLB_KING.csv')
    json_path = os.path.join(base_dir, 'mlb_king.json')

    # --- CSV (fix blank rows with lineterminator) ---
    csv_tmp = csv_path + '.writing'
    with open(csv_tmp, 'w', encoding='utf-8-sig', newline='') as f:
        df.to_csv(f, index=False, lineterminator='\n')
        f.flush()
        os.fsync(f.fileno())  # force OS-level flush to disk
    os.replace(csv_tmp, csv_path)

    # --- JSON ---
    json_tmp = json_path + '.writing'
    with open(json_tmp, 'w', encoding='utf-8') as f:
        df.to_json(f, orient='records', indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(json_tmp, json_path)

    print(f"\nSaved to {csv_path} ({len(df)} rows, {os.path.getsize(csv_path)} bytes)")
    print(f"Saved to {json_path} ({os.path.getsize(json_path)} bytes)")

    # --- Build dept3-1.json (Matchup Specialist input) ---
    dept31_path = os.path.join(base_dir, 'dept3-1.json')
    try:
        build_dept31_json(df, collector, today_edt, dept31_path)
    except Exception as e:
        print(f"  [dept3-1] Build failed: {e}")
        dept31_path = None

    # --- Build dept3-2.json (Situational Analyst input) ---
    dept32_path = os.path.join(base_dir, 'dept3-2.json')
    try:
        build_dept32_json(df, collector, today_edt, dept32_path)
    except Exception as e:
        print(f"  [dept3-2] Build failed: {e}")
        dept32_path = None

    # --- Auto push to GitHub (kirikariko/MLB) ---
    files_to_push = [csv_path, json_path]
    if dept31_path and os.path.exists(dept31_path):
        files_to_push.append(dept31_path)
    if dept32_path and os.path.exists(dept32_path):
        files_to_push.append(dept32_path)
    push_to_github(base_dir, files_to_push, today_edt)


# ============================================================
# DEPT3-2 HELPERS (momentum, fatigue, motivation, schedule context)
# ============================================================

# Historical rivalry pairs (canonical MLB rivalries)
RIVALRIES = {
    frozenset(['NYY', 'BOS']): 'NYY_BOS',
    frozenset(['LAD', 'SFG']): 'LAD_SFG',
    frozenset(['CHC', 'STL']): 'CHC_STL',
    frozenset(['NYM', 'PHI']): 'NYM_PHI',
    frozenset(['BAL', 'WSN']): 'BAL_WSN',
    frozenset(['CWS', 'CHC']): 'CWS_CHC',
    frozenset(['LAA', 'LAD']): 'LAA_LAD',
    frozenset(['NYM', 'NYY']): 'NYM_NYY',
    frozenset(['TEX', 'HOU']): 'TEX_HOU',
    frozenset(['SFG', 'OAK']): 'SFG_OAK',
    frozenset(['KCR', 'STL']): 'KCR_STL',
    frozenset(['CIN', 'CLE']): 'CIN_CLE',
    frozenset(['PIT', 'PHI']): 'PIT_PHI',
    frozenset(['DET', 'CWS']): 'DET_CWS',
    frozenset(['TBR', 'MIA']): 'TBR_MIA',
    # Added 2026-04-16
    frozenset(['LAD', 'SDP']): 'LAD_SDP',
    frozenset(['ATL', 'NYM']): 'ATL_NYM',
    frozenset(['BOS', 'TBR']): 'BOS_TBR',
}


def calc_record_str(schedule, team_id, n):
    """Get W-L record string for last N completed games."""
    completed = [g for g in schedule if g['status'] == 'Final']
    last_n = completed[-n:]
    w = sum(1 for g in last_n
            if (g['home_score'] if g['home_id'] == team_id else g['away_score'])
            > (g['away_score'] if g['home_id'] == team_id else g['home_score']))
    l = len(last_n) - w
    return f"{w}-{l}" if last_n else None


def calc_close_game_record(schedule, team_id, n=10):
    """W-L record in 1-run games from last N 1-run games."""
    close = []
    for g in schedule:
        if g['status'] != 'Final':
            continue
        if abs(g['home_score'] - g['away_score']) != 1:
            continue
        is_home = g['home_id'] == team_id
        my = g['home_score'] if is_home else g['away_score']
        opp = g['away_score'] if is_home else g['home_score']
        close.append(my > opp)
    last_n = close[-n:]
    if not last_n:
        return None
    w = sum(1 for x in last_n if x)
    return f"{w}-{len(last_n) - w}"


def calc_fatigue_context(schedule, team_id, today_home_abbr, game_date):
    """Rich fatigue data: yesterday location, doubleheader, L7 games, consec games."""
    today = datetime.strptime(game_date, '%Y-%m-%d')
    yesterday_str = (today - timedelta(days=1)).strftime('%Y-%m-%d')

    past = []
    for g in schedule:
        if g['status'] != 'Final':
            continue
        try:
            gd = datetime.strptime(g['date'], '%Y-%m-%d')
        except ValueError:
            continue
        if gd < today:
            past.append((gd, g))
    past.sort(key=lambda x: x[0])

    # Played yesterday + location
    played_yesterday = False
    yesterday_location = None
    for gd, g in past:
        if g['date'] == yesterday_str:
            played_yesterday = True
            yesterday_location = 'HOME' if g['home_id'] == team_id else 'AWAY'

    # Doubleheader: same-date games in schedule (any time today, including current)
    today_games_count = sum(1 for g in schedule if g['date'] == game_date
                            and (g['home_id'] == team_id or g['away_id'] == team_id))
    if today_games_count >= 2:
        # Check if there's an earlier game today already completed
        earlier_done = any(g for g in schedule
                           if g['date'] == game_date and g['status'] == 'Final'
                           and (g['home_id'] == team_id or g['away_id'] == team_id))
        doubleheader = 'GAME2' if earlier_done else 'GAME1'
    else:
        doubleheader = 'NONE'

    # Games in last 7 days (not counting today)
    seven_days_ago = today - timedelta(days=7)
    games_L7 = sum(1 for gd, _ in past if gd >= seven_days_ago)

    # Consecutive games without rest (back-to-back count ending on last past game)
    games_without_rest = 0
    if past:
        games_without_rest = 1
        for i in range(len(past) - 1, 0, -1):
            diff = (past[i][0] - past[i - 1][0]).days
            if diff <= 1:
                games_without_rest += 1
            else:
                break

    # Timezone change (last game venue vs today's venue)
    timezone_change = 0
    if past:
        last_home_id = past[-1][1]['home_id']
        last_abbr = MLB_ID_TO_ABBR.get(last_home_id)
        if last_abbr and today_home_abbr:
            tz_last = STADIUM_TIMEZONE.get(last_abbr)
            tz_now = STADIUM_TIMEZONE.get(today_home_abbr)
            if tz_last is not None and tz_now is not None:
                timezone_change = tz_now - tz_last

    return {
        'played_yesterday': played_yesterday,
        'yesterday_location': yesterday_location,
        'games_without_rest': games_without_rest,
        'doubleheader': doubleheader,
        'games_L7': games_L7,
        'timezone_change': timezone_change,
    }


def calc_series_context(home_wins, away_wins, game_num, is_home_team):
    """Derive series_position, series_record, sweep_risk/chance."""
    # Guess series length: typical MLB series is 3, sometimes 4 (or 2 for makeups)
    # Best-effort: if game_num >= 4, series_len = 4; else 3
    series_len = 4 if game_num >= 4 else 3

    # series_position: "2/3" etc
    series_position = f"{game_num}/{series_len}"

    # Team's own record in this series
    my_wins = home_wins if is_home_team else away_wins
    opp_wins = away_wins if is_home_team else home_wins
    series_record = f"{my_wins}-{opp_wins}"

    # Sweep risk: last game of series, down 0-2 or 0-3
    is_last_game = game_num == series_len
    sweep_risk = is_last_game and my_wins == 0 and opp_wins >= 2
    # Sweep chance: last game, leading 2-0 or 3-0
    sweep_chance = is_last_game and opp_wins == 0 and my_wins >= 2

    return {
        'series_position': series_position,
        'series_record': series_record,
        'sweep_risk': sweep_risk,
        'sweep_chance': sweep_chance,
    }


def get_rivalry_tag(home_abbr, away_abbr):
    """Return rivalry tag if today's matchup is a known rivalry."""
    return RIVALRIES.get(frozenset([home_abbr, away_abbr]))


def calc_schedule_context(date_str, year):
    """Calc days to all-star break, trade deadline, expanded roster status."""
    today = datetime.strptime(date_str, '%Y-%m-%d')
    # All-star break typically mid-July; use July 15 as proxy
    allstar = datetime(year, 7, 15)
    deadline = datetime(year, 7, 31)
    sep_1 = datetime(year, 9, 1)

    return {
        'days_to_allstar': (allstar - today).days,
        'days_to_deadline': (deadline - today).days,
        'expanded_roster': today >= sep_1,
    }


def build_dept31_json(df, collector, date_str, output_path):
    """Build dept3-1.json for Matchup Specialist.
    Combines: existing CSV fields + SP throws + team vs L/R OPS + bullpen fatigue.
    """
    games_out = []
    for _, row in df.iterrows():
        game_id = row['GAME_ID']
        home_abbr, away_abbr = game_id.split('_', 1)
        home_id = MLB_TEAM_IDS.get(home_abbr)
        away_id = MLB_TEAM_IDS.get(away_abbr)
        if not home_id or not away_id:
            continue

        # Find the live MLB game data for SP IDs
        home_sp_id = None
        away_sp_id = None
        for g in collector._today_games_cache or []:
            if g['home_abbr'] == home_abbr and g['away_abbr'] == away_abbr:
                home_sp_id = g['home_pitcher_id']
                away_sp_id = g['away_pitcher_id']
                break

        # SP throws (new lookup)
        home_throws = collector.mlb.get_pitcher_hand(home_sp_id)
        away_throws = collector.mlb.get_pitcher_hand(away_sp_id)

        # Team vs L/R OPS (from cached stats)
        def _ops(tid, sit):
            s = collector.mlb.get_team_stats(tid, 'hitting', sit)
            return safe_float(s.get('ops'))

        # Bullpen fatigue
        home_sched = collector.mlb.get_schedule(home_id)
        away_sched = collector.mlb.get_schedule(away_id)
        home_bp_fatigue = calc_bullpen_fatigue(home_sched, home_id, collector.mlb, date_str)
        away_bp_fatigue = calc_bullpen_fatigue(away_sched, away_id, collector.mlb, date_str)

        def _n(v):
            """Convert NaN/None to None for JSON."""
            if v is None:
                return None
            try:
                import math as _m
                if isinstance(v, float) and _m.isnan(v):
                    return None
            except Exception:
                pass
            return v

        games_out.append({
            'DATE': date_str,
            'GAME_ID': game_id,
            'home_team': home_abbr,
            'away_team': away_abbr,

            'pitcher': {
                'home_sp': {
                    'id': home_sp_id,
                    'throws': home_throws,
                    'pitch_mix': _n(row.get('HOME_SP_PitchMix')),
                    'xwOBA': _n(row.get('HOME_SP_xwOBA')),
                    'barrel_pct': _n(row.get('HOME_SP_Barrel')),
                    'hard_hit_pct': _n(row.get('HOME_SP_HardHit')),
                },
                'away_sp': {
                    'id': away_sp_id,
                    'throws': away_throws,
                    'pitch_mix': _n(row.get('AWAY_SP_PitchMix')),
                    'xwOBA': _n(row.get('AWAY_SP_xwOBA')),
                    'barrel_pct': _n(row.get('AWAY_SP_Barrel')),
                    'hard_hit_pct': _n(row.get('AWAY_SP_HardHit')),
                },
            },

            'batting_splits': {
                'home_team': {
                    'avg_vs_LHP': _n(row.get('AVG_BATTING_AVERAGE_HOMETEAM_VS LEFTPitcher')),
                    'avg_vs_RHP': _n(row.get('AVG_BATTING_AVERAGE_HOMETEAM_VS RightPitcher')),
                    'ops_vs_LHP': _ops(home_id, 'vl'),
                    'ops_vs_RHP': _ops(home_id, 'vr'),
                    'season_wOBA': _n(row.get('HOMETEAM_Woba')),
                },
                'away_team': {
                    'avg_vs_LHP': _n(row.get('AVG_BATTING_AVERAGE_AWAYTEAM_VS LEFTPitcher')),
                    'avg_vs_RHP': _n(row.get('AVG_BATTING_AVERAGE_AWAYTEAM_VS RightPitcher')),
                    'ops_vs_LHP': _ops(away_id, 'vl'),
                    'ops_vs_RHP': _ops(away_id, 'vr'),
                    'season_wOBA': _n(row.get('AWAYTEAM_wOBA')),
                },
            },

            'bullpen': {
                'home_team': {
                    'era_30d': _n(row.get('BP_ERA_30d_HOME')),
                    'irs_pct_30d': _n(row.get('BP_IRS_30d_HOME')),
                    'ip_3d': _n(row.get('BP_IP_3d_HOME')),
                    'workload_7d': _n(row.get('HOMETEAM_BULLPEN_WORKLOAD')),
                    'top_relievers': home_bp_fatigue,
                },
                'away_team': {
                    'era_30d': _n(row.get('BP_ERA_30d_AWAY')),
                    'irs_pct_30d': _n(row.get('BP_IRS_30d_AWAY')),
                    'ip_3d': _n(row.get('BP_IP_3d_AWAY')),
                    'workload_7d': _n(row.get('AWAYTEAM_BULLPEN_WORKLOAD')),
                    'top_relievers': away_bp_fatigue,
                },
            },
        })

    payload = {
        'DATE': date_str,
        'generated_at': datetime.now(tz=timezone.utc).isoformat(),
        'games': games_out,
    }

    # Atomic write
    tmp = output_path + '.writing'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, output_path)
    print(f"Saved to {output_path} ({len(games_out)} games, {os.path.getsize(output_path)} bytes)")


def build_dept32_json(df, collector, date_str, output_path):
    """Build dept3-2.json for Situational Analyst.
    Categories: momentum, fatigue, motivation, schedule_context.
    """
    standings = collector.mlb.get_standings()
    year = int(date_str[:4])
    sched_ctx = calc_schedule_context(date_str, year)

    def _n(v):
        """NaN/None → None for JSON."""
        if v is None:
            return None
        try:
            import math as _m
            if isinstance(v, float) and _m.isnan(v):
                return None
        except Exception:
            pass
        return v

    games_out = []
    for _, row in df.iterrows():
        game_id = row['GAME_ID']
        home_abbr, away_abbr = game_id.split('_', 1)
        home_id = MLB_TEAM_IDS.get(home_abbr)
        away_id = MLB_TEAM_IDS.get(away_abbr)
        if not home_id or not away_id:
            continue

        home_sched = collector.mlb.get_schedule(home_id)
        away_sched = collector.mlb.get_schedule(away_id)

        home_st = standings.get(home_id, {}) or {}
        away_st = standings.get(away_id, {}) or {}

        # --- ① Momentum ---
        home_momentum = {
            'record_L5': calc_record_str(home_sched, home_id, 5),
            'record_L10': calc_record_str(home_sched, home_id, 10),
            'streak': home_st.get('streak'),
            'close_game_L10': calc_close_game_record(home_sched, home_id, 10),
        }
        away_momentum = {
            'record_L5': calc_record_str(away_sched, away_id, 5),
            'record_L10': calc_record_str(away_sched, away_id, 10),
            'streak': away_st.get('streak'),
            'close_game_L10': calc_close_game_record(away_sched, away_id, 10),
        }

        # --- ② Fatigue ---
        home_fat = calc_fatigue_context(home_sched, home_id, home_abbr, date_str)
        away_fat = calc_fatigue_context(away_sched, away_id, home_abbr, date_str)

        # Travel miles → km (from existing CSV col)
        home_mi = _n(row.get('HOME_TRAVEL_MI'))
        away_mi = _n(row.get('AWAY_TRAVEL_MI'))
        home_fat['travel_distance_km'] = round(home_mi * 1.609) if home_mi is not None else 0
        away_fat['travel_distance_km'] = round(away_mi * 1.609) if away_mi is not None else 0

        # Reorder to match spec
        def _fat(d):
            return {
                'played_yesterday': d['played_yesterday'],
                'yesterday_location': d['yesterday_location'],
                'travel_distance_km': d['travel_distance_km'],
                'timezone_change': d['timezone_change'],
                'games_without_rest': d['games_without_rest'],
                'doubleheader': d['doubleheader'],
                'games_L7': d['games_L7'],
            }

        # --- ③ Motivation ---
        game_num = int(_n(row.get('Series_Game_Num')) or 1)
        home_wins = int(_n(row.get('Series_Home_Wins')) or 0)
        away_wins = int(_n(row.get('Series_Away_Wins')) or 0)
        # Home team's perspective for sweep logic
        home_series = calc_series_context(home_wins, away_wins, game_num, is_home_team=True)
        rivalry = get_rivalry_tag(home_abbr, away_abbr)

        motivation = {
            'home_team': {
                'division_rank': _n(home_st.get('division_rank')),
                'games_behind_leader': _n(home_st.get('games_back')),
                'wildcard_gb': _n(home_st.get('wildcard_gb')),
            },
            'away_team': {
                'division_rank': _n(away_st.get('division_rank')),
                'games_behind_leader': _n(away_st.get('games_back')),
                'wildcard_gb': _n(away_st.get('wildcard_gb')),
            },
            'series_position': home_series['series_position'],
            'series_record': f"{home_wins}-{away_wins}",
            'sweep_risk': home_series['sweep_risk'] or calc_series_context(home_wins, away_wins, game_num, False)['sweep_risk'],
            'sweep_chance': home_series['sweep_chance'] or calc_series_context(home_wins, away_wins, game_num, False)['sweep_chance'],
            'rivalry_tag': rivalry,
        }

        games_out.append({
            'DATE': date_str,
            'GAME_ID': game_id,
            'home_team': home_abbr,
            'away_team': away_abbr,
            'momentum': {
                'home_team': home_momentum,
                'away_team': away_momentum,
            },
            'fatigue': {
                'home_team': _fat(home_fat),
                'away_team': _fat(away_fat),
            },
            'motivation': motivation,
            'schedule_context': sched_ctx,
        })

    payload = {
        'DATE': date_str,
        'generated_at': datetime.now(tz=timezone.utc).isoformat(),
        'games': games_out,
    }

    tmp = output_path + '.writing'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, output_path)
    print(f"Saved to {output_path} ({len(games_out)} games, {os.path.getsize(output_path)} bytes)")


def push_to_github(repo_dir, files, date_str):
    """Stage, commit, and push specified files to GitHub.
    Silently no-ops if git is not available or remote is unreachable.
    """
    import subprocess

    def run(cmd):
        return subprocess.run(cmd, cwd=repo_dir, capture_output=True, text=True)

    # Verify git repo exists
    r = run(['git', 'rev-parse', '--is-inside-work-tree'])
    if r.returncode != 0:
        print(f"  [Git] Not a git repo — skipping push ({r.stderr.strip()})")
        return

    # Stage only the files we want
    rel_files = [os.path.relpath(f, repo_dir) for f in files]
    run(['git', 'add', '--'] + rel_files)

    # Check if anything actually changed
    r = run(['git', 'diff', '--cached', '--quiet'])
    if r.returncode == 0:
        print("  [Git] No changes to push")
        return

    # Commit
    msg = f"Auto-update MLB data for {date_str}"
    r = run(['git', 'commit', '-m', msg])
    if r.returncode != 0:
        print(f"  [Git] Commit failed: {r.stderr.strip()}")
        return

    # Push (try main, fall back to master)
    for branch in ('main', 'master'):
        r = run(['git', 'push', '-u', 'origin', f'HEAD:{branch}'])
        if r.returncode == 0:
            print(f"  [Git] Pushed to origin/{branch}: {msg}")
            return
    print(f"  [Git] Push failed: {r.stderr.strip()}")


if __name__ == '__main__':
    main()