"""Découpage des documents en passages (chunks) de taille bornée, avec chevauchement."""

import re

_SENTENCE_END = re.compile(r"(?<=[.!?…])\s+")


def _split_long(paragraph: str, max_chars: int) -> list[str]:
    """Découpe un paragraphe trop long par phrases, puis par mots en dernier recours."""
    pieces, current = [], ""
    for sentence in _SENTENCE_END.split(paragraph):
        while len(sentence) > max_chars:  # phrase géante : coupe à un espace
            cut = sentence.rfind(" ", 0, max_chars)
            cut = cut if cut > 0 else max_chars
            pieces.append(sentence[:cut].strip())
            sentence = sentence[cut:].strip()
        if current and len(current) + 1 + len(sentence) > max_chars:
            pieces.append(current)
            current = sentence
        else:
            current = f"{current} {sentence}" if current else sentence
    if current:
        pieces.append(current)
    return pieces


def _overlap_tail(text: str, overlap: int) -> str:
    """Fin du chunk précédent, recalée sur un début de mot."""
    if overlap <= 0 or len(text) <= overlap:
        return ""
    tail = text[-overlap:]
    space = tail.find(" ")
    return tail[space + 1:] if space >= 0 else tail


def split_text(text: str, max_chars: int = 1000, overlap: int = 150) -> list[str]:
    """Regroupe les paragraphes en chunks d'au plus ~max_chars caractères.

    Chaque nouveau chunk reprend les `overlap` derniers caractères du précédent,
    pour ne pas perdre le contexte d'une idée coupée en deux.
    """
    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        pieces = _split_long(paragraph, max_chars) if len(paragraph) > max_chars else [paragraph]
        for piece in pieces:
            if not current:
                current = piece
            elif len(current) + 1 + len(piece) <= max_chars:
                current = f"{current}\n{piece}"
            else:
                chunks.append(current)
                tail = _overlap_tail(current, overlap)
                current = f"{tail} {piece}" if tail and len(tail) + 1 + len(piece) <= max_chars else piece
    if current:
        chunks.append(current)
    return chunks
