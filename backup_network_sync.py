#!/usr/bin/env python3
r"""
Directory Sync Script (pathlib + logging + hashing + mirroring + threads)

Features:
- Pathlib for robust cross-OS path handling.
- Loguru logging to a rotating log file (console optional).
- Atomic copy (tmp -> replace) to avoid partial writes.
- Tolerant mtime comparison with slack.
- Content hashing (SHA-256):
    * Forced mode (--hash): always hash and copy only when content differs.
    * Fuzzy auto-hash (default): when size equal and mtimes within slack.
    * Disable fuzzy with --disable-hash.
- Mirroring (--mirror + --confirm-mirror or --assume-yes):
    * Deletes destination files not present at source (within same filters).
    * Non-interactive; prints/logs warnings and requires explicit confirmation flags.
- Threaded worker pool for IO-bound copying (--workers).
- Silent file-only logging by default. Use --no-progress-bar to disable tqdm and enable console logging.
- Backups:
    * Single timestamp directory per program run (shared by all jobs).
    * Back up ONLY destination files that will be overwritten or deleted (not every file).
    * Backup directory layout configurable; default puts files under:
          <backup_base>/<YYYY-MM-DD HH_MM_SS>/<source_basename>/...

INI per-section keys:
    [Settings]
    backup_dir = D:\Backups
    backup_layout = basename
    backup_subdir_format = %Y-%m-%d %H_%M_%S

    [Job A]
    source = O:\Source\Path
    destination = N:\Dest\Path
    extensions = .txt, .csv         ; or "txt,csv" or "*" for all
    exclude_globs = *.tmp,~$*,*.bak
    backup = true
    mirror = false
"""

from __future__ import annotations

import argparse
import configparser
import datetime as dt
import hashlib
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from tqdm.auto import tqdm  # better backend selection on Windows/VSCode

try:
    from loguru import logger
except ImportError:
    print("Error: loguru is required. Install it with: pip install loguru", file=sys.stderr)
    sys.exit(1)


# ------------------------------- Data Types --------------------------------- #

@dataclass(frozen=True)
class SyncPlan:
    """A single sync job loaded from the INI."""
    section_name: str
    source: Path
    destination: Path
    extensions: Optional[Sequence[str]]
    backup_enabled: bool
    exclude_globs: Sequence[str]
    mirror_enabled: bool


@dataclass(frozen=True)
class CopyOptions:
    """Immutable options passed to worker tasks."""
    dry_run: bool
    mtime_slack_seconds: float
    force_hash: bool
    auto_hash_enabled: bool
    hash_chunk_bytes: int


@dataclass(frozen=True)
class GlobalSettings:
    """Global (per-run) settings from [Settings]."""
    backup_layout: str            # 'basename' | 'full'
    backup_subdir_format: str     # e.g. "%Y-%m-%d %H_%M_%S"


# ------------------------------ Small Utilities ----------------------------- #

def _anchor_label_for_backup(src: Path) -> str:
    """For 'full' layout: turn filesystem anchor into a safe label."""
    anchor = src.anchor
    if not anchor:
        return "root"
    label = anchor.replace("\\", "_").replace("/", "_").replace(":", "")
    label = label.strip("_")
    return label or "root"


def _sanitize_component(name: str) -> str:
    """Remove path separators and stray colons to keep Windows happy."""
    return name.replace("\\", "_").replace("/", "_").replace(":", "_").strip()


def is_tty() -> bool:
    """True if stdout is an interactive terminal."""
    return sys.stdout.isatty()


def normalize_extensions(exts: Optional[Sequence[str]]) -> Optional[List[str]]:
    """
    Ensure leading dot (case-insensitive), drop empties. "*" or None => no filter.
    """
    if not exts:
        return None
    cleaned: List[str] = []
    for e in exts:
        e = (e or "").strip()
        if not e:
            continue
        if e == "*":
            return None
        if not e.startswith("."):
            e = "." + e
        cleaned.append(e.lower())
    return cleaned or None


def parse_csv(s: str) -> List[str]:
    if not s:
        return []
    return [part.strip() for part in s.split(",") if part.strip()]


def safe_resolve(p: Path) -> Path:
    try:
        return p.expanduser().resolve(strict=False)
    except Exception:
        return p.expanduser().absolute()


def path_is_within(child: Path, parent: Path) -> bool:
    child_r = safe_resolve(child)
    parent_r = safe_resolve(parent)
    try:
        child_r.relative_to(parent_r)
        return True
    except ValueError:
        return False


def configure_logging(log_file: Path, level: str, console_enabled: bool) -> None:
    """
    Configure loguru sinks.

    - File sink: always enabled (rotates at ~10 MB, keeps 10 files).
    - Console sink: enabled only when console_enabled=True.
      (We set console_enabled when --no-progress-bar is used and not --silent.)
    """
    logger.remove()

    # Console sink only when explicitly allowed (no progress bars).
    if console_enabled:
        logger.add(sys.stdout, level=level, enqueue=True, backtrace=False, diagnose=False)

    # File sink (always on)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(log_file),
        level=level,
        rotation="10 MB",
        retention=10,
        encoding="utf-8",
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )


# ------------------------------ Core Functions ------------------------------ #

def _make_extset(extensions: Optional[Sequence[str]]) -> Optional[set[str]]:
    return set(e.lower() for e in (extensions or [])) or None


def enumerate_files_live(
    root: Path,
    extensions: Optional[Sequence[str]],
    exclude_globs: Sequence[str],
    *,
    show_progress: bool,
    log_prefix: str = "Enumerating files",
    heartbeat_secs: float = 0.5,       # periodic repaint even within one huge dir
    redraw_every_files: int = 512,     # repaint after N files even if < heartbeat
) -> List[Path]:
    """
    Enumerate files with:
      - extension/exclude filters
      - frequent liveness updates (time + file-count driven)
      - responsive Ctrl-C
      - single output mode (bar OR plaintext OR silent)
    """
    root = safe_resolve(root)
    extset = _make_extset(extensions)
    results: List[Path] = []

    dirs_scanned = 0
    files_seen = 0
    matched = 0

    last_beat = time.monotonic()
    last_files_seen = 0

    # Decide output mode once
    use_bar = bool(show_progress)
    use_plain = (not show_progress) and (not logger._core.handlers)  # we’ll print only if no console logger
    # We’ll be conservative: prefer bar; otherwise plaintext only if the user wanted "no bar" but not silent.
    # (If you want plaintext even when console logging is enabled, set use_plain = (not show_progress) and not args.silent)

    bar = None
    if use_bar:
        # Send tqdm to stderr so it never fights stdout logs
        bar = tqdm(
            total=0,
            unit=" dirs",
            desc=log_prefix,
            dynamic_ncols=True,
            leave=True,
            mininterval=0.05,
            smoothing=0.0,
            disable=False,
            file=sys.stderr,
        )
    elif use_plain:
        print(f"[{log_prefix}] starting ...", flush=True)

    def heartbeat():
        nonlocal last_beat, last_files_seen
        now = time.monotonic()
        need_time = (now - last_beat) >= heartbeat_secs
        need_files = (files_seen - last_files_seen) >= redraw_every_files
        if not (need_time or need_files):
            return
        if bar is not None:
            bar.set_postfix(dirs=dirs_scanned, files=files_seen, matches=matched)
            bar.update(0)
            bar.refresh()
        elif use_plain:
            print(f"[{log_prefix}] dirs={dirs_scanned} files={files_seen} matches={matched}", flush=True)
        if need_time:
            last_beat = now
        if need_files:
            last_files_seen = files_seen

    stack: List[Path] = [root]
    while stack:
        current_dir = stack.pop()
        try:
            for entry in current_dir.iterdir():
                # Let Ctrl-C abort immediately
                if entry.is_dir():
                    stack.append(entry)
                elif entry.is_file():
                    files_seen += 1
                    # Extension filter
                    if extset and entry.suffix.lower() not in extset:
                        heartbeat()
                        continue
                    # Exclude filter on relative path
                    if exclude_globs:
                        try:
                            rel = entry.relative_to(root).as_posix()
                        except Exception:
                            rel = entry.name
                        if any(Path(rel).match(pat) for pat in exclude_globs):
                            heartbeat()
                            continue
                    results.append(entry)
                    matched += 1
                heartbeat()
        except PermissionError as e:
            logger.warning("Permission denied entering dir: {p} :: {e}", p=current_dir, e=e)
        except OSError as e:
            logger.warning("OS error entering dir: {p} :: {e}", p=current_dir, e=e)
        finally:
            dirs_scanned += 1
            if bar is not None:
                bar.update(1)
                bar.set_postfix(dirs=dirs_scanned, files=files_seen, matches=matched)
                bar.update(0)
                bar.refresh()
            elif use_plain:
                print(f"[{log_prefix}] dirs={dirs_scanned} files={files_seen} matches={matched}", flush=True)

    if bar is not None:
        bar.close()

    logger.info("Enumeration complete: dirs={d}, files_seen={f}, matches={m}",
                d=dirs_scanned, f=files_seen, m=matched)
    return results

def iter_files(
    root: Path,
    extensions: Optional[Sequence[str]],
    exclude_globs: Sequence[str],
) -> Iterable[Path]:
    """Simple iterator used for destination filtering in mirror step."""
    root = safe_resolve(root)
    extset = _make_extset(extensions)
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if extset and p.suffix.lower() not in extset:
            continue
        if exclude_globs:
            rel = p.relative_to(root).as_posix()
            if any(Path(rel).match(pattern) for pattern in exclude_globs):
                continue
        yield p


def compute_sha256(path: Path, chunk_bytes: int = 1 << 20) -> str:
    """Compute SHA-256 digest of a file in chunks (default 1 MiB)."""
    h = hashlib.sha256()
    with path.open("rb", buffering=64 * 1024) as fh:
        while True:
            chunk = fh.read(chunk_bytes)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def should_update_file_with_hash(
    src: Path,
    dst: Path,
    opts: CopyOptions,
) -> Tuple[bool, str]:
    """
    Decide whether to copy src -> dst.
    Returns (should_copy, reason) where reason is one of:
      "dst_missing", "mtime_newer", "size_diff", "hash_diff", "same", "hash_error", "perm_error"
    """
    try:
        if not dst.exists():
            return True, "dst_missing"

        s_stat = src.stat()
        d_stat = dst.stat()

        # Forced hashing: copy only if content differs
        if opts.force_hash:
            try:
                sh = compute_sha256(src, opts.hash_chunk_bytes)
                dh = compute_sha256(dst, opts.hash_chunk_bytes)
                return (sh != dh, "hash_diff" if sh != dh else "same")
            except OSError as e:
                logger.warning("Hashing error; falling back to mtime/size: {e}", e=e)

        # Fast checks
        if s_stat.st_size != d_stat.st_size:
            return True, "size_diff"

        mtime_diff = s_stat.st_mtime - d_stat.st_mtime
        if mtime_diff > opts.mtime_slack_seconds:
            return True, "mtime_newer"

        # Fuzzy window (equal size, mtimes within slack): optionally hash
        if abs(mtime_diff) <= opts.mtime_slack_seconds and opts.auto_hash_enabled:
            try:
                sh = compute_sha256(src, opts.hash_chunk_bytes)
                dh = compute_sha256(dst, opts.hash_chunk_bytes)
                return (sh != dh, "hash_diff" if sh != dh else "same")
            except OSError as e:
                logger.warning("Fuzzy hashing error; treating as copy for safety: {e}", e=e)
                return True, "hash_error"

        return False, "same"
    except FileNotFoundError:
        return True, "dst_missing"
    except PermissionError as e:
        logger.warning("Permission error during stat; will attempt copy: {e}", e=e)
        return True, "perm_error"


def copy_file_atomic(src: Path, dst: Path, *, dry_run: bool) -> None:
    """Atomic copy via temporary file then replace."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        logger.info("[DRY-RUN] copy '{src}' -> '{dst}'", src=src, dst=dst)
        return

    tmp = dst.with_suffix(dst.suffix + ".copy_tmp")
    try:
        shutil.copy2(src, tmp)
        tmp.replace(dst)  # atomic on Windows & POSIX
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass


def backup_dest_file(dest_file: Path, backup_root: Path, plan_destination: Path, *, dry_run: bool) -> None:
    """
    Save the current destination file version into the backup tree.
    Layout: <backup_root>/<rel-to-destination>
    """
    rel = dest_file.relative_to(plan_destination)
    target = backup_root / rel
    if dry_run:
        logger.info("[DRY-RUN] backup '{src}' -> '{dst}'", src=dest_file, dst=target)
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(dest_file, target)


def sync_files_threaded(
    source: Path,
    destination: Path,
    files: List[Path],
    opts: CopyOptions,
    *,
    show_progress: bool,
    progress_desc: str,
    backup_root: Optional[Path] = None,   # if set, back up destination files that are overwritten
) -> Tuple[int, int, int, int]:
    """
    Copy/update a list of files using a worker pool.
    Returns (copied_count, skipped_count, error_count, backed_up_count).
    """
    total = len(files)
    copied = 0
    skipped = 0
    errors = 0
    backed_up = 0

    def worker(src_file: Path) -> Tuple[str, Path, Path, bool]:
        """Per-file task: may back up existing dest, then copy."""
        rel = src_file.relative_to(source)
        dst_file = destination / rel
        try:
            do_copy, reason = should_update_file_with_hash(src_file, dst_file, opts)
            if do_copy:
                did_backup = False
                if backup_root is not None and dst_file.exists():
                    try:
                        backup_dest_file(dst_file, backup_root, destination, dry_run=opts.dry_run)
                        did_backup = True
                    except Exception as e:
                        # Don't fail the sync because backup failed; log and continue
                        logger.error("Backup error: {src} -> {dst} :: {err}", src=dst_file,
                                     dst=(backup_root / rel), err=e)
                copy_file_atomic(src_file, dst_file, dry_run=opts.dry_run)
                logger.debug("Copied ({reason}): {src} -> {dst}", reason=reason, src=src_file, dst=dst_file)
                return ("copied", src_file, dst_file, did_backup)
            else:
                logger.trace("Skipped (same): {src} -> {dst}", src=src_file, dst=dst_file)
                return ("skipped", src_file, dst_file, False)
        except Exception as e:
            logger.error("Copy error: {src} -> {dst} :: {err}", src=src_file, dst=dst_file, err=e)
            return ("error", src_file, dst_file, False)

    bar = None
    if show_progress:
        bar = tqdm(total=total, desc=progress_desc, dynamic_ncols=True, disable=False)

    backlog_limit = max(1, sync_files_threaded.backlog_limit)  # type: ignore[attr-defined]
    with ThreadPoolExecutor(max_workers=sync_files_threaded.max_workers, thread_name_prefix="sync") as ex:  # type: ignore[attr-defined]
        it = iter(files)
        in_flight: Dict[object, Path] = {}

        # Seed
        try:
            for _ in range(min(backlog_limit, total)):
                sf = next(it)
                fut = ex.submit(worker, sf)
                in_flight[fut] = sf
        except StopIteration:
            pass

        while in_flight:
            done, _ = wait(in_flight.keys(), return_when=FIRST_COMPLETED)
            for fut in done:
                in_flight.pop(fut, None)
                status, _s, _d, did_backup = fut.result()
                if status == "copied":
                    copied += 1
                    if did_backup:
                        backed_up += 1
                elif status == "skipped":
                    skipped += 1
                else:
                    errors += 1
                if bar:
                    bar.update(1)

            # Top up
            try:
                while len(in_flight) < backlog_limit:
                    sf = next(it)
                    fut = ex.submit(worker, sf)
                    in_flight[fut] = sf
            except StopIteration:
                pass

    if bar:
        bar.close()

    return copied, skipped, errors, backed_up


# Attach overridable attributes for worker tuning (set by caller)
sync_files_threaded.max_workers = 32     # type: ignore[attr-defined]
sync_files_threaded.backlog_limit = 128  # type: ignore[attr-defined]


def compute_backup_dir(
    backup_base: Path,
    source: Path,
    *,
    layout: str,
    date_fmt: str,
    when: Optional[dt.datetime] = None,
) -> Path:
    """
    layout = 'basename' -> <base>/<date>/<src.name>/...
           = 'full'     -> <base>/<date>/<anchor>/<rel_to_root>/...
    date_fmt example: '%Y-%m-%d %H_%M_%S'
    """
    when = when or dt.datetime.now()
    date_part = _sanitize_component(when.strftime(date_fmt))
    src_resolved = safe_resolve(source)

    if layout == "basename":
        return safe_resolve(backup_base) / date_part / _sanitize_component(src_resolved.name)
    else:
        anchor_label = _anchor_label_for_backup(src_resolved)
        try:
            rel = src_resolved.relative_to(src_resolved.anchor)
        except Exception:
            rel = Path(src_resolved.name)
        return safe_resolve(backup_base) / date_part / anchor_label / rel


def validate_source_and_destination(source: Path, destination: Path) -> Optional[str]:
    if not source.exists():
        return f"Source directory '{source}' does not exist."
    if not source.is_dir():
        return f"Source path '{source}' is not a directory."
    if path_is_within(destination, source):
        return (
            "Refusing to sync into a subdirectory of the source.\n"
            f"  source:      {safe_resolve(source)}\n"
            f"  destination: {safe_resolve(destination)}"
        )
    return None


def run_sync_job(
    plan: SyncPlan,
    backup_base_dir: Path,
    opts: CopyOptions,
    *,
    show_progress: bool,
    mirror: bool,
    mirror_confirmed: bool,
    workers: int,
    backup_layout: str,
    backup_subdir_format: str,
    run_when: dt.datetime,   # <-- single timestamp for ALL jobs in this run
) -> None:
    logger.info("Section '{sec}': source='{src}', dest='{dst}', backup={b}, mirror={m}",
                sec=plan.section_name, src=plan.source, dst=plan.destination,
                b=plan.backup_enabled, m=mirror)

    err = validate_source_and_destination(plan.source, plan.destination)
    if err:
        logger.error("{msg}", msg=err)
        return

    # -------- Enumeration with live updates & responsive Ctrl-C --------
    try:
        files = enumerate_files_live(
            plan.source,
            plan.extensions,
            plan.exclude_globs,
            show_progress=show_progress,
            log_prefix=f"Enumerating: {plan.section_name}",
        )
    except KeyboardInterrupt:
        logger.warning("Enumeration cancelled by user (Ctrl-C). Skipping section '{sec}'.",
                       sec=plan.section_name)
        return

    total = len(files)
    logger.info("Section '{sec}': {n} candidate files.", sec=plan.section_name, n=total)

    # Ensure destination root exists (even in dry-run we still may need rel path calcs)
    plan.destination.mkdir(parents=True, exist_ok=True)

    # Compute the per-plan backup root for THIS RUN (single timestamp across all jobs)
    backup_root_for_plan: Optional[Path] = None
    if plan.backup_enabled:
        backup_root_for_plan = compute_backup_dir(
            backup_base_dir, plan.source,
            layout=backup_layout,
            date_fmt=backup_subdir_format,
            when=run_when,
        )
        # Safety: never back up into/under the source
        if safe_resolve(backup_root_for_plan) == safe_resolve(plan.source) or path_is_within(backup_root_for_plan, plan.source):
            logger.error("Refusing to back up into/under source. backup_dir='{bd}' source='{src}'",
                         bd=backup_root_for_plan, src=plan.source)
            backup_root_for_plan = None  # disable backups for this plan

    # Thread pool settings
    sync_files_threaded.max_workers = workers  # type: ignore[attr-defined]
    sync_files_threaded.backlog_limit = max(4, workers * 4)  # type: ignore[attr-defined]

    # Main sync pass (with optional "backup old dest before overwrite")
    copied, skipped, errors, backed_up = sync_files_threaded(
        plan.source, plan.destination, files, opts,
        show_progress=show_progress, progress_desc="Syncing files",
        backup_root=backup_root_for_plan,
    )
    logger.info("Sync: copied={c}, skipped={s}, errors={e}, backed_up={b}",
                c=copied, s=skipped, e=errors, b=backed_up)

    # Mirroring (delete extras) with pre-delete backup of destination files
    if mirror:
        if not mirror_confirmed:
            logger.error(
                "Mirror requested but not confirmed. Add '--confirm-mirror' or '--assume-yes' to proceed."
            )
            return

        logger.warning(
            "MIRRORING enabled for section '{sec}'. Files in destination that do not exist in source "
            "AND match your filters WILL BE DELETED.",
            sec=plan.section_name,
        )

        src_rel_set = {f.relative_to(plan.source).as_posix() for f in files}
        dest_files_filtered = list(iter_files(plan.destination, plan.extensions, plan.exclude_globs))

        deletions = 0
        del_errors = 0
        predelete_backups = 0
        show_bar = show_progress and bool(dest_files_filtered)
        bar = tqdm(total=len(dest_files_filtered), desc="Mirroring (delete extras)",
                   dynamic_ncols=True, disable=not show_bar)

        for dfile in dest_files_filtered:
            rel = dfile.relative_to(plan.destination).as_posix()
            if rel not in src_rel_set:
                try:
                    # Pre-delete backup of destination file if plan.backup_enabled
                    if plan.backup_enabled and backup_root_for_plan is not None and dfile.exists():
                        try:
                            backup_dest_file(dfile, backup_root_for_plan, plan.destination, dry_run=opts.dry_run)
                            predelete_backups += 1
                        except Exception as e:
                            logger.error("Backup (pre-delete) error: {p} :: {e}", p=dfile, e=e)

                    if opts.dry_run:
                        logger.info("[DRY-RUN] delete '{p}'", p=dfile)
                    else:
                        dfile.unlink(missing_ok=True)
                    deletions += 1
                except Exception as e:
                    logger.error("Delete error: {p} :: {e}", p=dfile, e=e)
                    del_errors += 1
            if bar:
                bar.update(1)
        if bar:
            bar.close()

        logger.info("Mirror: deleted={d}, errors={e}, predelete_backups={b}",
                    d=deletions, e=del_errors, b=predelete_backups)


# ---------------------------------- CLI / INI -------------------------------- #

def load_plans_from_ini(ini_path: Path) -> Tuple[Path, List[SyncPlan], GlobalSettings]:
    cfg = configparser.ConfigParser(interpolation=None)
    read_ok = cfg.read(ini_path)
    if not read_ok:
        raise SystemExit(f"[ERROR] Failed to read config file: {ini_path}")

    if "Settings" not in cfg:
        raise SystemExit("[ERROR] Missing [Settings] section in config.")

    backup_dir_str = cfg.get("Settings", "backup_dir", fallback="").strip()
    if not backup_dir_str:
        raise SystemExit("[ERROR] 'backup_dir' not specified in [Settings].")
    backup_base = safe_resolve(Path(backup_dir_str))

    # Global (optional) settings
    ini_layout = cfg.get("Settings", "backup_layout", fallback="basename").strip().lower()
    if ini_layout not in ("basename", "full"):
        ini_layout = "basename"

    ini_fmt = cfg.get("Settings", "backup_subdir_format",
                      fallback="%Y-%m-%d %H_%M_%S", raw=True).strip()
    if not ini_fmt:
        ini_fmt = "%Y-%m-%d %H_%M_%S"

    settings = GlobalSettings(
        backup_layout=ini_layout,
        backup_subdir_format=ini_fmt,
    )

    plans: List[SyncPlan] = []
    for section in cfg.sections():
        if section == "Settings":
            continue

        src_str = cfg[section].get("source", "").strip()
        dst_str = cfg[section].get("destination", "").strip()
        if not src_str or not dst_str:
            logger.warning("Skipping section '{s}': missing source or destination.", s=section)
            continue

        # Parse & normalize lists
        exts_raw = cfg[section].get("extensions", "")
        exts = normalize_extensions(parse_csv(exts_raw))

        exclude_globs = parse_csv(cfg[section].get("exclude_globs", ""))

        backup = cfg[section].getboolean("backup", fallback=True)
        mirror_enabled = cfg[section].getboolean("mirror", fallback=False)

        plans.append(
            SyncPlan(
                section_name=section,
                source=safe_resolve(Path(src_str)),
                destination=safe_resolve(Path(dst_str)),
                extensions=exts,                 # None => all extensions
                backup_enabled=backup,
                exclude_globs=exclude_globs,
                mirror_enabled=mirror_enabled,
            )
        )

    if not plans:
        logger.warning("No sync sections found in config.")

    return backup_base, plans, settings


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Directory Sync (pathlib + loguru + hash + mirror + threads)")
    p.add_argument("-c", "--config", default="config.ini", help="Path to config.ini (default: ./config.ini)")
    p.add_argument("-d", "--dry-run", action="store_true", help="Simulate actions without making changes")

    # Backup layout / timestamp formatting
    p.add_argument("--backup-layout", choices=["basename", "full"], default=None,
                   help="Backup folder layout. 'basename' -> <base>/<date>/<src.name>/... ; "
                        "'full' -> anchor + full path.")
    p.add_argument("--backup-subdir-format", default=None,
                   help="strftime format for the dated backup subfolder. "
                        "Default: '%%Y-%%m-%%d %%H_%%M_%%S'")

    # Logging
    p.add_argument("--log-file", type=Path, default=Path("sync.log"), help="Log file path (default: ./sync.log)")
    p.add_argument("--log-level", default="INFO", choices=["TRACE","DEBUG","INFO","WARNING","ERROR","CRITICAL"],
                   help="Log level (default: INFO)")
    p.add_argument("--silent", action="store_true", help="No console output or progress bars (still logs).")
    p.add_argument("--no-progress-bar", action="store_true",
                   help="Disable all tqdm progress bars. Console logging is enabled when this is set.")

    # Hashing / comparison
    p.add_argument("--mtime-slack", type=float, default=1.0,
                   help="Seconds of mtime slack to tolerate (default: 1.0)")
    p.add_argument("--hash", dest="force_hash", action="store_true",
                   help="Force SHA-256 content hashing for equality check.")
    p.add_argument("--disable-hash", dest="disable_auto_hash", action="store_true",
                   help="Disable fuzzy auto-hash when sizes equal and mtimes within slack.")
    p.add_argument("--hash-chunk", type=int, default=1 << 20,
                   help="Hash chunk size in bytes (default: 1048576)")

    # Threading
    p.add_argument("--workers", type=int, default=32, help="Number of worker threads (default: 32)")

    # Mirroring
    p.add_argument("--mirror", action="store_true",
                   help="Delete destination files not present in source (restricted to filters).")
    p.add_argument("--no-mirror", action="store_true",
                   help="Disable mirroring even if set in INI.")
    p.add_argument("--confirm-mirror", action="store_true",
                   help="Acknowledge that mirroring deletes files; required to proceed.")
    p.add_argument("--assume-yes", action="store_true",
                   help="Alias for --confirm-mirror (non-interactive scripting).")

    return p


def main() -> None:
    args = build_arg_parser().parse_args()

    # Console logging is enabled ONLY when --no-progress-bar is set and not --silent
    console_log_enabled = (not args.silent) and bool(args.no_progress_bar)

    # Configure logging before reading config to capture errors
    configure_logging(args.log_file, args.log_level, console_enabled=console_log_enabled)
    logger.info("Starting sync. Config='{cfg}', dry_run={dr}, silent={si}, no_progress_bar={npb}",
                cfg=args.config, dr=args.dry_run, si=args.silent, npb=args.no_progress_bar)

    ini_path = safe_resolve(Path(args.config))
    if not ini_path.exists():
        logger.error("Config file not found: {p}", p=ini_path)
        raise SystemExit(2)

    backup_base, plans, ini_settings = load_plans_from_ini(ini_path)

    # Effective settings (CLI overrides INI)
    eff_layout = args.backup_layout or ini_settings.backup_layout
    eff_fmt    = args.backup_subdir_format or ini_settings.backup_subdir_format

    # Single per-run timestamp used for ALL jobs
    run_when = dt.datetime.now()

    # Ensure backup base directory exists unless dry-run.
    # (We create per-plan subfolders lazily on first backup.)
    if args.dry_run:
        logger.info("[DRY-RUN] Would ensure backup base directory: '{p}'", p=backup_base)
    else:
        backup_base.mkdir(parents=True, exist_ok=True)

    # Effective mirroring flags
    mirror_cli = bool(args.mirror)
    no_mirror_cli = bool(args.no_mirror)
    mirror_confirmed = bool(args.confirm_mirror or args.assume_yes)

    # Build common copy options
    opts = CopyOptions(
        dry_run=args.dry_run,
        mtime_slack_seconds=args.mtime_slack,
        force_hash=args.force_hash,
        auto_hash_enabled=not args.disable_auto_hash,
        hash_chunk_bytes=max(64 * 1024, args.hash_chunk),  # minimum 64 KiB
    )

    # show bars whenever allowed (even if not a true TTY)
    show_progress = (not args.silent) and (not args.no_progress_bar)


    # Run all plans
    for plan in plans:
        effective_mirror = (plan.mirror_enabled or mirror_cli) and not no_mirror_cli

        if effective_mirror and not mirror_confirmed:
            logger.error(
                "Section '{sec}': mirroring requested but not confirmed. "
                "Add '--confirm-mirror' or '--assume-yes' to proceed.",
                sec=plan.section_name,
            )
            effective_mirror = False

        run_sync_job(
            plan,
            backup_base_dir=backup_base,
            opts=opts,
            show_progress=show_progress,
            mirror=effective_mirror,
            mirror_confirmed=mirror_confirmed,
            workers=max(1, args.workers),
            backup_layout=eff_layout,
            backup_subdir_format=eff_fmt,
            run_when=run_when,   # <-- single timestamp for the entire run
        )

    logger.info("All sections complete.")


if __name__ == "__main__":
    main()
