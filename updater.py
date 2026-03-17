"""
updater.py — автообновление EDI Message Sender с GitHub.

Схема работы:
  1. Читаем локальный version.json → текущая версия
  2. GET version_url (raw.githubusercontent.com) → актуальная версия
  3. Если версии совпадают или нет сети — ничего не делаем
  4. Если доступна новее — предлагаем обновление пользователю
  5. Скачиваем ZIP, распаковываем поверх текущей папки
  6. Перезапускаем main.py

Для выпуска новой версии:
  - Обновите version в репозитории version.json: {"version": "1.1.0", ...}
  - Загрузите новый EdiMessageSender.zip в GitHub Releases
    (Releases → Create release → Upload assets)
"""

import json
import sys
import zipfile
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

# ── Константы ─────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).resolve().parent
VERSION_FILE = BASE_DIR / "version.json"
TIMEOUT      = 8  # секунд на HTTP-запрос

# Файлы и папки, которые НИКОГДА не перезаписываются при обновлении
PRESERVE = {
    "edi_config.json",
    "edi_store.json",
    ".token_cache.json",
    "edi_documents",
    "logs",
    "joker.png",
}


# ── Версионирование ───────────────────────────────────────────────────────────

def _parse_version(v: str) -> tuple:
    """'1.2.3' → (1, 2, 3)"""
    try:
        return tuple(int(x) for x in str(v).strip().split("."))
    except ValueError:
        return (0,)


def _local_version() -> Optional[str]:
    if not VERSION_FILE.exists():
        return None
    try:
        return json.loads(VERSION_FILE.read_text(encoding="utf-8")).get("version")
    except Exception:
        return None


def _remote_info(version_url: str) -> Optional[dict]:
    """Получить version.json с GitHub. Возвращает dict или None при ошибке."""
    try:
        import urllib.request
        req = urllib.request.Request(
            version_url,
            headers={"User-Agent": "EdiMessageSender-Updater/1.0"},
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


# ── Загрузка ──────────────────────────────────────────────────────────────────

def _download(url: str, dest: Path) -> bool:
    """Скачать файл по URL в dest с progress-баром."""
    try:
        import urllib.request

        def _progress(count, block, total):
            if total > 0:
                pct = min(100, int(count * block * 100 / total))
                bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                print(f"\r  [{bar}] {pct}%", end="", flush=True)

        urllib.request.urlretrieve(url, str(dest), reporthook=_progress)
        print()
        return True
    except Exception as exc:
        print(f"\n  Ошибка загрузки: {exc}")
        return False


# ── Установка ─────────────────────────────────────────────────────────────────

def _install_zip(zip_path: Path, target_dir: Path) -> bool:
    """
    Распаковать ZIP в target_dir, пропуская файлы из PRESERVE.
    ZIP может содержать вложенную папку верхнего уровня — она прозрачно убирается.
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = zf.infolist()

            # Определяем общий префикс (папка верхнего уровня в архиве)
            prefix = ""
            top_dirs = {m.filename.split("/")[0]
                        for m in members if "/" in m.filename}
            if len(top_dirs) == 1:
                candidate = top_dirs.pop() + "/"
                if all(m.filename.startswith(candidate)
                       for m in members if not m.is_dir()):
                    prefix = candidate

            for member in members:
                if member.is_dir():
                    continue

                # Убираем префикс
                rel = member.filename[len(prefix):] if prefix else member.filename
                if not rel:
                    continue

                # Защита: пропускаем файлы из PRESERVE
                top = Path(rel).parts[0] if Path(rel).parts else rel
                if top in PRESERVE:
                    continue

                dest = target_dir / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(zf.read(member.filename))

        return True
    except Exception as exc:
        print(f"  Ошибка распаковки: {exc}")
        return False


# ── Публичный API ─────────────────────────────────────────────────────────────

def check_and_update(silent: bool = False) -> None:
    """
    Проверить наличие обновления и при согласии пользователя — установить.

    silent=True: если нет обновления или нет сети — ничего не печатает.
    """
    local_v = _local_version()
    if not local_v:
        if not silent:
            print("  version.json не найден — проверка обновлений недоступна.")
        return

    try:
        cfg = json.loads(VERSION_FILE.read_text(encoding="utf-8"))
        version_url = cfg.get("version_url", "")
        zip_url     = cfg.get("zip_url", "")
    except Exception:
        return

    if not version_url:
        return

    if not silent:
        print("  Проверка обновлений...", end="", flush=True)

    remote = _remote_info(version_url)

    if remote is None:
        # Нет сети или репозиторий недоступен — тихо продолжаем
        if not silent:
            print(" сервер недоступен.")
        return

    remote_v   = remote.get("version", "0.0.0")
    remote_url = remote.get("zip_url", zip_url)

    if _parse_version(remote_v) <= _parse_version(local_v):
        if not silent:
            print(f" установлена актуальная версия {local_v}.")
        return

    # ── Доступна новая версия ─────────────────────────────────────────────────
    print(f"\n  ┌─────────────────────────────────────────────┐")
    print(f"  │  Доступно обновление: {local_v} → {remote_v:<21}│")
    print(f"  └─────────────────────────────────────────────┘")

    ans = input("  Установить сейчас? (y/n): ").strip().lower()
    if ans != "y":
        print("  Обновление отложено.\n")
        return

    # ── Скачиваем во временную папку, затем устанавливаем ────────────────────
    with tempfile.TemporaryDirectory() as tmp:
        zip_path = Path(tmp) / "update.zip"
        print(f"  Загружаю обновление...")
        if not _download(remote_url, zip_path):
            print("  Обновление не выполнено.")
            return

        print("  Устанавливаю...")
        if not _install_zip(zip_path, BASE_DIR):
            print("  Обновление не выполнено.")
            return

    # Обновляем версию в локальном version.json
    try:
        data = json.loads(VERSION_FILE.read_text(encoding="utf-8"))
        data["version"] = remote_v
        if remote.get("zip_url"):
            data["zip_url"] = remote["zip_url"]
        VERSION_FILE.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass

    print(f"  ✓ Версия {remote_v} установлена.")
    print("  Перезапуск...\n")

    # Перезапускаем и выходим
    subprocess.Popen([sys.executable] + sys.argv)
    sys.exit(0)
