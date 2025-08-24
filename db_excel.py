# db_excel.py — base Excel v2 (equipes, amistosos, sets, rallies)
from __future__ import annotations
from pathlib import Path
from datetime import datetime
import pandas as pd

SHEETS = ["equipes", "jogadoras", "amistosos", "sets", "rallies"]

def _empty_frames(team_name: str = "Nosso Time") -> dict[str, pd.DataFrame]:
    return {
        "equipes": pd.DataFrame([{"team_id": 1, "team_name": team_name}]),
        "jogadoras": pd.DataFrame(columns=["team_id", "player_number", "player_name", "position"]),
        "amistosos": pd.DataFrame(columns=[
            "match_id","date","home_team_id","away_team_id",
            "home_sets","away_sets","notes","status","finished_at"
        ]),
        "sets": pd.DataFrame(columns=["set_id","match_id","set_number","home_points","away_points","winner_team_id"]),
        "rallies": pd.DataFrame(columns=[
            "rally_id","match_id","set_number","rally_no","side","position",
            "player_number","action","result","who_scored",
            "score_home","score_away","raw_text"
        ]),
    }

def init_or_load(db_path: Path, team_name: str = "Nosso Time") -> dict[str, pd.DataFrame]:
    """Cria (se preciso) e carrega todas as abas esperadas."""
    db_path = Path(db_path)
    if not db_path.exists():
        frames = _empty_frames(team_name)
        save_all(db_path, frames)
        return frames

    # Lê o que existe e garante colunas obrigatórias
    xls = pd.ExcelFile(db_path)
    frames: dict[str, pd.DataFrame] = {}
    for s in SHEETS:
        if s in xls.sheet_names:
            frames[s] = pd.read_excel(xls, s)
        else:
            frames[s] = _empty_frames(team_name)[s]

    # Colunas que podem faltar em bases antigas
    if "status" not in frames["amistosos"].columns:
        frames["amistosos"]["status"] = "OPEN"
    if "finished_at" not in frames["amistosos"].columns:
        frames["amistosos"]["finished_at"] = pd.NaT
    for col in ["home_points","away_points","winner_team_id"]:
        if col not in frames["sets"].columns:
            frames["sets"][col] = 0 if "points" in col else None
    for col in ["score_home","score_away","raw_text"]:
        if col not in frames["rallies"].columns:
            if col == "raw_text":
                frames["rallies"][col] = ""
            else:
                frames["rallies"][col] = 0
    return frames

def save_all(db_path: Path, frames: dict[str, pd.DataFrame]) -> None:
    """Grava todas as abas no Excel (sobrescreve)."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(db_path, engine="openpyxl") as wr:
        for s, df in frames.items():
            df.to_excel(wr, index=False, sheet_name=s)

def _next_id(series: pd.Series | None) -> int:
    if series is None or series.empty or series.max() != series.max():  # NaN
        return 1
    try:
        return int(series.max()) + 1
    except Exception:
        return 1

def ensure_team(frames: dict[str, pd.DataFrame], team_name: str) -> int:
    eq = frames["equipes"]
    if not eq.empty and (eq["team_name"] == team_name).any():
        return int(eq.loc[eq["team_name"] == team_name, "team_id"].iloc[0])
    tid = _next_id(eq["team_id"] if "team_id" in eq.columns else None)
    frames["equipes"] = pd.concat(
        [eq, pd.DataFrame([{"team_id": tid, "team_name": team_name}])],
        ignore_index=True
    )
    return tid

def add_match(frames: dict[str, pd.DataFrame], date: str, home_team: str, away_team: str, notes: str = "") -> int:
    """Adiciona um amistoso com status OPEN e sets=0x0. Retorna match_id."""
    home_id = ensure_team(frames, home_team)
    away_id = ensure_team(frames, away_team)
    mt = frames["amistosos"]
    mid = _next_id(mt["match_id"] if "match_id" in mt.columns else None)
    row = {
        "match_id": mid, "date": date, "home_team_id": home_id, "away_team_id": away_id,
        "home_sets": 0, "away_sets": 0, "notes": notes, "status": "OPEN", "finished_at": pd.NaT
    }
    frames["amistosos"] = pd.concat([mt, pd.DataFrame([row])], ignore_index=True)
    return mid

def add_set(frames: dict[str, pd.DataFrame], match_id: int, set_number: int) -> int:
    """Cria um set com pontuações zeradas."""
    st = frames["sets"]
    sid = _next_id(st["set_id"] if "set_id" in st.columns else None)
    row = {"set_id": sid, "match_id": match_id, "set_number": set_number,
           "home_points": 0, "away_points": 0, "winner_team_id": None}
    frames["sets"] = pd.concat([st, pd.DataFrame([row])], ignore_index=True)
    return sid

def append_rally(frames: dict[str, pd.DataFrame], match_id: int, set_number: int, row: dict) -> None:
    """Acrescenta um rally; atualiza score_home/score_away e a linha do set."""
    rls = frames["rallies"]
    cur = rls[(rls["match_id"] == match_id) & (rls["set_number"] == set_number)]
    last_home = int(cur.iloc[-1]["score_home"]) if not cur.empty else 0
    last_away = int(cur.iloc[-1]["score_away"]) if not cur.empty else 0
    last_no = int(cur.iloc[-1]["rally_no"]) if not cur.empty else 0

    who = row.get("who_scored")
    if who == "NOS":
        last_home += 1
    elif who == "ADV":
        last_away += 1

    nxt = {
        "rally_id": _next_id(rls["rally_id"] if "rally_id" in rls.columns else None),
        "match_id": match_id,
        "set_number": set_number,
        "rally_no": last_no + 1,
        "side": row.get("side", ""),
        "position": row.get("position", ""),
        "player_number": row.get("player_number"),
        "action": row.get("action", ""),
        "result": row.get("result", ""),
        "who_scored": who,
        "score_home": last_home,
        "score_away": last_away,
        "raw_text": row.get("raw_text", ""),
    }
    frames["rallies"] = pd.concat([rls, pd.DataFrame([nxt])], ignore_index=True)

    # atualiza pontuação agregada do set
    st = frames["sets"]
    mask = (st["match_id"] == match_id) & (st["set_number"] == set_number)
    st.loc[mask, "home_points"] = last_home
    st.loc[mask, "away_points"] = last_away
    frames["sets"] = st

def last_open_match(frames: dict[str, pd.DataFrame]) -> int | None:
    """Retorna o match_id da última partida em OPEN (ou None)."""
    mt = frames["amistosos"]
    if mt.empty:
        return None
    # Adiciona a coluna status se não existir
    if "status" not in mt.columns:
        mt["status"] = "OPEN"
        
    open_rows = mt[mt["status"] == "OPEN"].sort_values("match_id", ascending=False)
    if open_rows.empty:
        return None
    return int(open_rows.iloc[0]["match_id"])

def finalize_match(frames: dict[str, pd.DataFrame], match_id: int) -> None:
    """Marca partida como CLOSED e define finished_at."""
    mt = frames["amistosos"]
    mask = (mt["match_id"] == int(match_id))
    mt.loc[mask, "status"] = "CLOSED"
    mt.loc[mask, "finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    frames["amistosos"] = mt
