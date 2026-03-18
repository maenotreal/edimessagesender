"""
updater.py — автообновление EDI Message Sender с GitHub.

Схема работы:
  1. Читаем локальный version.json → текущая версия
  2. GET version_url (raw.githubusercontent.com) → актуальная версия
  3. Если версии совпадают или нет сети — ничего не делаем
  4. Если доступна новее — предлагаем обновление пользователю
  5а. Режим EXE  — скачиваем edimessagesender.exe, заменяем через bat-скрипт
  5б. Режим скрипт — скачиваем ZIP, распаковываем поверх текущей папки
  6. Перезапускаем приложение

Для выпуска новой версии:
  - Обновите version в репозитории version.json: {"version": "1.1.0", ...}
  - GitHub Actions автоматически соберёт EdiMessageSender.zip и
    edimessagesender.exe и прикрепит их к релизу
"""

import hashlib
import json
import sys
import zipfile
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

# ── Режим запуска ─────────────────────────────────────────────────────────────
IS_FROZEN = getattr(sys, "frozen", False)   # True когда запущен как .exe

# ── Константы ─────────────────────────────────────────────────────────────────
BASE_DIR     = (Path(sys.executable).resolve().parent
                if IS_FROZEN else Path(__file__).resolve().parent)
VERSION_FILE = BASE_DIR / "version.json"
TIMEOUT      = 8  # секунд на HTTP-запрос

# Файлы и папки, которые НИКОГДА не перезаписываются при обновлении (ZIP-режим)
PRESERVE = {
    "edi_config.json",
    "edi_store.json",
    ".token_cache.json",
    "edi_documents",
    "logs",
}


# ── Версионирование ───────────────────────────────────────────────────────────

def _parse_version(v: str) -> tuple:
    """'1.2.3' → (1, 2, 3)"""
    try:
        return tuple(int(x) for x in str(v).strip().split("."))
    except ValueError:
        return (0,)


def _bundled_version_file() -> Optional[Path]:
    """Путь к version.json внутри PyInstaller-бандла (только чтение)."""
    if IS_FROZEN:
        p = Path(getattr(sys, "_MEIPASS", "")) / "version.json"
        if p.exists():
            return p
    return None


def _read_version_data() -> Optional[dict]:
    """Читает version.json: сначала рядом с EXE/скриптом, затем из бандла."""
    for source in (VERSION_FILE, _bundled_version_file()):
        if source and source.exists():
            try:
                return json.loads(source.read_text(encoding="utf-8"))
            except Exception:
                continue
    return None


def _local_version() -> Optional[str]:
    data = _read_version_data()
    return data.get("version") if data else None


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
    """Скачать файл потоково с progress-баром."""
    try:
        import requests as req
        resp = req.get(url, stream=True, timeout=TIMEOUT,
                       headers={"User-Agent": "EdiMessageSender-Updater/1.0"})
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0:
                    pct = min(100, int(downloaded * 100 / total))
                    bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                    print(f"\r  [{bar}] {pct}%", end="", flush=True)
        print()
        return True
    except Exception as exc:
        print(f"\n  Ошибка загрузки: {exc}")
        return False


def _verify_sha256(path: Path, expected: str) -> bool:
    """Проверить SHA256 файла. Пустой expected — пропустить проверку."""
    if not expected:
        return True
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest().lower() == expected.lower()


# ── Установка: EXE-режим ──────────────────────────────────────────────────────

def _install_exe(new_exe_path: Path) -> bool:
    """
    Заменить текущий exe на скачанный через bat-скрипт.

    Bat ждёт завершения текущего процесса, затем переименовывает
    новый exe поверх старого и перезапускает приложение.
    """
    try:
        import shutil
        current_exe = Path(sys.executable).resolve()
        staged_exe  = current_exe.with_name(current_exe.stem + "_update.exe")
        bat_path    = current_exe.parent / "_edi_update.bat"

        shutil.copy2(new_exe_path, staged_exe)

        bat_path.write_text(
            "@echo off\n"
            "timeout /t 2 /nobreak >NUL\n"
            ":retry\n"
            f'move /Y "{staged_exe}" "{current_exe}" >NUL 2>&1\n'
            "if errorlevel 1 (\n"
            "  timeout /t 1 /nobreak >NUL\n"
            "  goto retry\n"
            ")\n"
            f'start "" "{current_exe}"\n'
            'del "%~f0"\n',
            encoding="ascii",
        )

        subprocess.Popen(
            ["cmd", "/c", str(bat_path)],
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        return True
    except Exception as exc:
        print(f"  Ошибка установки: {exc}")
        return False


# ── Установка: ZIP-режим ──────────────────────────────────────────────────────

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
    cfg = _read_version_data()
    if not cfg:
        if not silent:
            print("  version.json не найден — проверка обновлений недоступна.")
        return

    local_v     = cfg.get("version", "")
    version_url = cfg.get("version_url", "")
    zip_url     = cfg.get("zip_url", "")
    exe_url     = cfg.get("exe_url", "")

    if not local_v:
        return

    if not version_url:
        return

    if not silent:
        print("  Проверка обновлений...", end="", flush=True)

    remote = _remote_info(version_url)

    if remote is None:
        if not silent:
            print(" сервер недоступен.")
        return

    remote_v = remote.get("version", "0.0.0")

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

    print(f"  Загружаю обновление...")

    if IS_FROZEN:
        # ── EXE-режим ─────────────────────────────────────────────────────────
        remote_exe_url = remote.get("exe_url", exe_url)
        if not remote_exe_url:
            print("  exe_url не найден в version.json — обновление недоступно.")
            return

        with tempfile.TemporaryDirectory() as tmp:
            dl_path = Path(tmp) / "edimessagesender_new.exe"
            if not _download(remote_exe_url, dl_path):
                print("  Обновление не выполнено.")
                return

            if not _verify_sha256(dl_path, remote.get("sha256_exe", "")):
                print("  Ошибка: контрольная сумма не совпадает — файл повреждён или подменён.")
                return

            print("  Устанавливаю...")
            if not _install_exe(dl_path):
                print("  Обновление не выполнено.")
                return

    else:
        # ── ZIP-режим (скрипт) ────────────────────────────────────────────────
        remote_zip_url = remote.get("zip_url", zip_url)
        if not remote_zip_url:
            print("  zip_url не найден в version.json — обновление недоступно.")
            return

        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "update.zip"
            if not _download(remote_zip_url, zip_path):
                print("  Обновление не выполнено.")
                return

            if not _verify_sha256(zip_path, remote.get("sha256_zip", "")):
                print("  Ошибка: контрольная сумма не совпадает — файл повреждён или подменён.")
                return

            print("  Устанавливаю...")
            if not _install_zip(zip_path, BASE_DIR):
                print("  Обновление не выполнено.")
                return

    # ── Обновляем локальный version.json ──────────────────────────────────────
    try:
        data = json.loads(VERSION_FILE.read_text(encoding="utf-8"))
        data["version"] = remote_v
        if remote.get("zip_url"):
            data["zip_url"] = remote["zip_url"]
        if remote.get("exe_url"):
            data["exe_url"] = remote["exe_url"]
        VERSION_FILE.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass

    print(f"  ✓ Версия {remote_v} установлена.")
    print("  Перезапуск...\n")

    if IS_FROZEN:
        sys.exit(0)
    else:
        subprocess.Popen([sys.executable] + sys.argv)
        sys.exit(0)
