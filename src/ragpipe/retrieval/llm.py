"""Génération de la réponse par un LLM, via une API compatible OpenAI (/chat/completions).

Par défaut Ollama sur la machine hôte ; il suffit de changer LLM_BASE_URL, LLM_API_KEY
et LLM_MODEL pour passer à Mistral, Groq, OpenAI, etc.
"""

import json
from collections.abc import Iterator

import httpx

from ragpipe.config import settings
from ragpipe.retrieval.search import Source

SYSTEM_PROMPT = """Tu es un assistant de recherche documentaire.
Réponds en français, uniquement à partir des extraits fournis.
Cite tes sources entre crochets, par exemple [1] ou [2][3], juste après l'information concernée.
Si les extraits ne permettent pas de répondre, dis-le clairement au lieu d'inventer."""


def build_messages(question: str, sources: list[Source]) -> list[dict]:
    context = "\n\n".join(f"[{i}] {s.title}\n{s.text}" for i, s in enumerate(sources, 1))
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Extraits :\n\n{context}\n\nQuestion : {question}"},
    ]


def stream_answer(question: str, sources: list[Source], temperature: float = 0.2) -> Iterator[str]:
    """Renvoie la réponse morceau par morceau (streaming SSE)."""
    payload = {
        "model": settings.llm_model,
        "messages": build_messages(question, sources),
        "temperature": temperature,
        "stream": True,
    }
    headers = {"Authorization": f"Bearer {settings.llm_api_key}"}
    with httpx.stream("POST", f"{settings.llm_base_url}/chat/completions",
                      json=payload, headers=headers, timeout=180) as response:
        response.raise_for_status()
        for line in response.iter_lines():
            if not line.startswith("data: "):
                continue
            data = line.removeprefix("data: ").strip()
            if data == "[DONE]":
                break
            choices = json.loads(data).get("choices") or [{}]
            delta = choices[0].get("delta", {}).get("content")
            if delta:
                yield delta
