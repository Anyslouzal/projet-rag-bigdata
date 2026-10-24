from ragpipe.processing.chunking import split_text
from ragpipe.processing.ray_job import chunk_uuid, to_chunks


def test_court_texte_un_seul_chunk():
    assert split_text("Un paragraphe.\nUn autre.", max_chars=100) == ["Un paragraphe.\nUn autre."]


def test_texte_vide():
    assert split_text("   \n\n ") == []


def test_taille_maximale_respectee():
    text = "\n".join(f"Phrase numéro {i} du paragraphe. " * 8 for i in range(40))
    chunks = split_text(text, max_chars=500, overlap=80)
    assert len(chunks) > 5
    assert all(len(c) <= 500 for c in chunks)


def test_chevauchement_entre_chunks():
    paragraphs = [f"Paragraphe {i} " + "mot " * 40 for i in range(10)]
    chunks = split_text("\n".join(paragraphs), max_chars=400, overlap=60)
    # le début de chaque chunk reprend la fin du précédent
    for previous, current in zip(chunks, chunks[1:]):
        assert current.split()[0] in previous[-60:]


def test_mot_geant_decoupe():
    chunks = split_text("x" * 2500, max_chars=1000, overlap=0)
    assert [len(c) for c in chunks] == [1000, 1000, 500]


def test_identifiants_deterministes():
    assert chunk_uuid(42, 3) == chunk_uuid(42, 3)
    assert chunk_uuid(42, 3) != chunk_uuid(42, 4)


def test_to_chunks_conserve_les_metadonnees():
    doc = {"doc_id": 7, "title": "Paris", "url": "https://fr.wikipedia.org/wiki/Paris",
           "content": "Paris est la capitale de la France.\n" * 80}
    chunks = to_chunks(doc)
    assert [c["chunk_index"] for c in chunks] == list(range(len(chunks)))
    assert {c["title"] for c in chunks} == {"Paris"}
    assert len({c["chunk_id"] for c in chunks}) == len(chunks)
