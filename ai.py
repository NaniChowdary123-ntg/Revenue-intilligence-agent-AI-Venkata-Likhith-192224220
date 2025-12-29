import json
import requests
from typing import Any, Dict
from .config import OPENAI_API_KEY, OPENAI_MODEL

def ai_text(system: str, user: str, max_tokens: int = 400) -> str:
    if not OPENAI_API_KEY:
        return ""

    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": OPENAI_MODEL,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=25)
    r.raise_for_status()
    data = r.json()
    return (data["choices"][0]["message"]["content"] or "").strip()

def ai_json(system: str, user: str, schema_hint: str) -> Dict[str, Any]:
    txt = ai_text(system, user + "\n\nReturn ONLY valid JSON.\nSchema hint:\n" + schema_hint, max_tokens=700)
    if not txt:
        return {}
    try:
        return json.loads(txt)
    except Exception:
        if "{" in txt and "}" in txt:
            txt2 = txt[txt.find("{"): txt.rfind("}") + 1]
            try:
                return json.loads(txt2)
            except Exception:
                return {}
        return {}
