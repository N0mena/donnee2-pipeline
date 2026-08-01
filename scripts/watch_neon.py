import argparse
import hashlib
import time
from pathlib import Path

from .commun import RACINE_PROJET, logger
from .load_warehouse import CLEAN_FILE
from .sync_neon import sync

ETAT_FILE = RACINE_PROJET / "data" / ".last_sync_sha256"


def hash_fichier(chemin: Path) -> str | None:
    if not chemin.exists():
        return None
    h = hashlib.sha256()
    with open(chemin, "rb") as f:
        for bloc in iter(lambda: f.read(65536), b""):
            h.update(bloc)
    return h.hexdigest()


def lire_etat() -> str | None:
    if not ETAT_FILE.exists():
        return None
    return ETAT_FILE.read_text(encoding="utf-8").strip()


def ecrire_etat(hachage: str) -> None:
    ETAT_FILE.parent.mkdir(parents=True, exist_ok=True)
    ETAT_FILE.write_text(hachage + "\n", encoding="utf-8")


def synchroniser_si_changement() -> bool:
    """Sync si le CSV a changé depuis le dernier sync. Retourne True si un sync a eu lieu."""
    hachage = hash_fichier(CLEAN_FILE)
    if hachage is None:
        logger.info("CSV %s absent, rien à synchroniser.", CLEAN_FILE.name)
        return False

    precedent = lire_etat()
    if precedent == hachage:
        return False

    logger.info("Changement détecté sur %s, synchronisation en cours…", CLEAN_FILE.name)
    sync(CLEAN_FILE)
    ecrire_etat(hachage)
    logger.info("Synchronisation terminée (état mis à jour).")
    return True


def surveiller(interval: int) -> None:
    logger.info("Surveillance de %s (intervalle %ds)…", CLEAN_FILE.resolve(), interval)
    while True:
        try:
            hachage = hash_fichier(CLEAN_FILE)
            if hachage is None:
                time.sleep(interval)
                continue
            # attend que le fichier soit stable (évite un sync sur un CSV en cours d'écriture)
            time.sleep(min(interval, 5))
            if hash_fichier(CLEAN_FILE) != hachage:
                continue
            if lire_etat() != hachage:
                sync(CLEAN_FILE)
                ecrire_etat(hachage)
        except Exception as e:
            logger.error("Erreur pendant la surveillance : %s", e)
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description="Surveille le CSV et resynchronise Neon.")
    parser.add_argument("--daemon", action="store_true", help="Surveiller en continu.")
    parser.add_argument(
        "--interval", type=int, default=60,
        help="Intervalle de vérification en secondes (défaut : 60).",
    )
    args = parser.parse_args()

    if args.daemon:
        surveiller(args.interval)
    else:
        synchroniser_si_changement()


if __name__ == "__main__":
    main()
