# parser_free.py — parser flexível para linhas do scout
from __future__ import annotations
import re

POSICOES = {
    r"\bponteir[ao]?\b": "PONTA",
    r"\bopost[ao]?\b": "OPPOSTA",
    r"\bcentral\b|\bmeio\b": "CENTRAL",
    r"\blevantador[ao]?\b": "LEV",
    r"\bl[íi]ber[oa]\b|\bliber[oa]\b": "LÍB",
    r"\brecep(c|ç)ao\b|\brecep(c|ç)[aã]o\b": "RECEPÇÃO",
}

SUBTIPOS = {
    r"\bdiagonal\b": "DIAGONAL",
    r"\blinha\b": "LINHA",
    r"\bpipe\b": "PIPE",
    r"\bsegunda\b": "SEGUNDA",
    r"\bmeio\b": "MEIO",
    r"\bbloqueio\b": "BLOQUEIO",
    r"\blob\b": "LOB",
    r"\bponto\b": "PONTO",
}

def _find_first(text: str, mapping: dict) -> str | None:
    for pat, val in mapping.items():
        if re.search(pat, text, flags=re.IGNORECASE):
            return val
    return None

def parse_line(raw: str) -> dict | None:
    """Interpreta uma linha de scout. Regras:
       - Prefixo 1 = NOS; 0/adv = ADV; sem prefixo -> NOS.
       - Se contém 'erro', ponto vai para o outro lado.
       - Tenta pegar posição (ponteira/oposta/central/levantadora/libero),
         número da jogadora (1–99) e ação (diagonal, linha, pipe, segunda, meio, bloqueio, lob, ponto).
    """
    t = raw.strip()
    if not t:
        return None

    side = None  # NOS/ADV
    text = t
    if re.match(r"^\s*1\b", t):
        side = "NOS"
        text = re.sub(r"^\s*1\s*", "", t, flags=re.IGNORECASE)
    elif re.match(r"^\s*0\b", t) or re.match(r"^\s*adv\b", t, flags=re.IGNORECASE):
        side = "ADV"
        text = re.sub(r"^\s*(0|adv)\s*", "", t, flags=re.IGNORECASE)
    else:
        side = "NOS"

    posicao = _find_first(text, POSICOES) or ("ADVERSÁRIO" if side == "ADV" else "")
    mnum = re.search(r"\b(\d{1,2})\b", text)
    numero = int(mnum.group(1)) if mnum else None
    action = _find_first(text, SUBTIPOS) or ""
    is_erro = bool(re.search(r"\berro\b", text, flags=re.IGNORECASE))

    if is_erro:
        result = "ERRO" if side == "NOS" else "ERRO_ADV"
        who_scored = "ADV" if side == "NOS" else "NOS"
    else:
        result = "PONTO" if side == "NOS" else "PONTO_ADV"
        who_scored = side

    return {
        "side": side,
        "position": posicao if posicao else ("SACA" if action == "SAQUE" else ""),
        "player_number": numero,
        "action": action,
        "result": result,
        "who_scored": who_scored,
        "raw_text": t,
    }
