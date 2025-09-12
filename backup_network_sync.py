#!/usr/bin/env python3
"""
Directory Sync Script (pathlib edition)

- Uses pathlib for all path handling
- Atomic copy to avoid half-written targets
- Tolerant mtime comparisons to handle coarse filesystem timestamps
- Optional backup copy into a date-stamped directory tree
- Optional exclude glob patterns per section (e.g., "*.tmp,~$*")
- Robust progress reporting with tqdm
- Defensive checks (source/destination existence, subpath containment, etc.)
"""

from __future__ import annotations

import argparse
import configparser
import datetime as dt
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from tqdm import tqdm


# ------------------------------- Data Types --------------------------------- #

@dataclass(frozen=True)
class SyncPlan:
    """Simple value object that describes a single sync job."""
    section_name: str
    source: Path
    destination: Path
    extensions: Optional[Sequence[str]]
    backup_enabled: bool
    exclude_globs: Sequence[str]


# ------------------------------ Small Utilities ----------------------------- #

def is_tty() -> bool:
    """Return True if stdout is a TTY (i.e., interactive terminal)."""
    return sys.stdout.isatty()


def normalize_extensions(exts: Optional[Sequence[str]]) -> Optional[List[str]]:
    """
    @brief Normalize extensions: ensure leading dot, lower-case, drop empties.
    @param exts Optional sequence of extensions; "*" or None means "all".
    @return Normalized list or None if no filtering should be applied.
    """
    if not exts:
        return None
    cleaned: List[str] = []
    for e in exts:
        e = (e or "").strip()
        if not e:
            continue
        if e == "*":
            return None  # "*" means no filtering
        if not e.startswith("."):
            e = "." + e
        cleaned.append(e.lower())
    return cleaned or None


def parse_csv(s: str) -> List[str]:
    """
    @brief Parse a simple comma-separated string into a list of trimmed items.
    @param s The input string (possibly empty).
    @return List of items (may be empty).
    """
    if not s:
        return []
    return [part.strip() for part in s.split(",") if part.strip()]


def safe_resolve(p: Path) -> Path:
    """
    @brief Resolve a path without failing on non-existent parents.
    @param p The path to resolve.
    @return Resolved absolute path (best effort).
    """
    try:
        return p.expanduser().resolve(strict=False)
    except Exception:
        # Fallback: expand user and make absolute
        return p.expanduser().absolute()


def path_is_within(child: Path, parent: Path) -> bool:
    """
    @brief True if `child` is located inside `parent` (after resolving).
    """
    child_r = safe_resolve(child)
    parent_r = safe_resolve(parent)
    try:
        child_r.relative_to(parent_r)
        return True
    except ValueError:
        return False


# ------------------------------ Core Functions ------------------------------ #

def iter_files(
    root: Path,
    extensions: Optional[Sequence[str]],
    exclude_globs: Sequence[str],
) -> Iterable[Path]:
    """
    @brief Yield all files in `root` respecting extension and exclude filters.
    @param root The root directory to walk.
    @param extensions Optional set/list of extensions (e.g., [".txt", ".log"]); None = all.
    @param exclude_globs Sequence of glob patterns to exclude (applied to relative path).
    @yield Path objects for files under root.
    """
    root = safe_resolve(root)
    extset = set(e.lower() for e in (extensions or []))

    for p in root.rglob("*"):
        # Only files
        if not p.is_file():
            continue

        # Extension filter (case-insensitive)
        if extset and p.suffix.lower() not in extset:
            continue

        # Exclusion filter (match against relative path string)
        if exclude_globs:
            rel = p.relative_to(root).as_posix()
            excluded = any(Path(rel).match(pattern) for pattern in exclude_globs)
            if excluded:
                continue

        yield p


def should_update_file(
    src: Path,
    dst: Path,
    *,
    mtime_slack_seconds: float = 1.0,
    compare_size: bool = True,
) -> bool:
    """
    @brief Decide whether a file should be updated based on metadata.
    @param src Source file path.
    @param dst Destination file path.
    @param mtime_slack_seconds Allowable timestamp slack to handle coarse fs mtimes.
    @param compare_size If True, also compare sizes.
    @return True if we should copy `src` to `dst`.
    """
    try:
        if not dst.exists():
            return True

        s_stat = src.stat()
        d_stat = dst.stat()

        # Compare mtime with tolerance
        mtime_diff = s_stat.st_mtime - d_stat.st_mtime
        if mtime_diff > mtime_slack_seconds:
            return True

        # Optionally compare size
        if compare_size and s_stat.st_size != d_stat.st_size:
            return True

        return False
    except FileNotFoundError:
        # Destination vanished mid-check; copy it.
        return True
    except PermissionError:
        # Safer to attempt update (may fail later, but we tried).
        return True


def copy_file_atomic(src: Path, dst: Path, *, dry_run: bool) -> None:
    """
    @brief Copy `src` to `dst` atomically: copy to temp file then replace.
    @param src Source file path.
    @param dst Destination file path (parent is created as needed).
    @param dry_run When True, do not modify the filesystem.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        print(f"[DRY-RUN] Would copy '{src}' -> '{dst}'")
        return

    # Temp file in same directory for atomic replace
    tmp = dst.with_suffix(dst.suffix + ".copy_tmp")
    try:
        shutil.copy2(src, tmp)
        # Path.replace is atomic on POSIX and Windows
        tmp.replace(dst)
    finally:
        # Best-effort cleanup if something went wrong before replace
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass


def sync_once(
    source: Path,
    destination: Path,
    extensions: Optional[Sequence[str]],
    exclude_globs: Sequence[str],
    *,
    dry_run: bool,
    desc: str,
) -> None:
    """
    @brief Sync (copy/update) files from source into destination one pass.
    @param source Source directory.
    @param destination Destination directory (created as needed).
    @param extensions Optional list of normalized extensions; None = all.
    @param exclude_globs Glob patterns to exclude.
    @param dry_run If True, print actions without making changes.
    @param desc Label for the tqdm progress bar.
    """
    files = list(iter_files(source, extensions, exclude_globs))
    total = len(files)

    destination.mkdir(parents=True, exist_ok=True)

    with tqdm(
        total=total, desc=desc, dynamic_ncols=True, disable=not is_tty()
    ) as bar:
        for src_file in files:
            rel = src_file.relative_to(source)
            dst_file = destination / rel

            try:
                if should_update_file(src_file, dst_file):
                    copy_file_atomic(src_file, dst_file, dry_run=dry_run)
            except PermissionError as e:
                print(f"[WARN] Permission error: {src_file} -> {dst_file}: {e}")
            except OSError as e:
                print(f"[WARN] OS error: {src_file} -> {dst_file}: {e}")
            finally:
                bar.update(1)


def compute_backup_dir(backup_base: Path, source: Path, when: Optional[dt.datetime] = None) -> Path:
    """
    @brief Compute a dated backup directory path for a given source path.

    The layout under `backup_base` is:
      YYYY-MM-DD/<drive-or-root>/<path-relative-to-root>

    Examples:
      Windows:  base/2025-09-12/C/Users/Alice/Documents/Project
      POSIX:    base/2025-09-12/root/home/alice/Documents/Project

    @param backup_base Base directory where backups are stored.
    @param source Source directory being backed up.
    @param when Optional timestamp for the date folder; defaults to now.
    @return Destination Path for the backup tree root for this source.
    """
    when = when or dt.datetime.now()
    date_dir = when.strftime("%Y-%m-%d")

    src_resolved = safe_resolve(source)
    anchor = Path(src_resolved.anchor).as_posix().strip("/\\")
    if not anchor:
        # POSIX root "/" yields empty after strip; name it "root"
        anchor = "root"

    # Strip the anchor from the source to get a path relative to filesystem root
    try:
        relative_to_root = src_resolved.relative_to(src_resolved.anchor)
    except Exception:
        # Should not happen, but fall back to name-only
        relative_to_root = Path(src_resolved.name)

    return safe_resolve(backup_base) / date_dir / anchor / relative_to_root


def validate_source_and_destination(source: Path, destination: Path) -> Optional[str]:
    """
    @brief Validate source/destination pair. Returns an error string or None if OK.
    """
    if not source.exists():
        return f"Source directory '{source}' does not exist."

    if not source.is_dir():
        return f"Source path '{source}' is not a directory."

    # Destination may not exist yet; we only check for obvious foot-guns
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
    *,
    dry_run: bool,
) -> None:
    """
    @brief Execute a single sync job (optionally with backup pass first).
    @param plan The SyncPlan describing this job.
    @param backup_base_dir Base directory for backups.
    @param dry_run If True, only print intended actions.
    """
    print(f"\nProcessing section: {plan.section_name}")

    # Validate
    err = validate_source_and_destination(plan.source, plan.destination)
    if err:
        print(f"[ERROR] {err} Skipping.")
        return

    # Optional backup pass
    if plan.backup_enabled:
        backup_dir = compute_backup_dir(backup_base_dir, plan.source)
        if dry_run:
            print(f"[DRY-RUN] Would create backup directory '{backup_dir}'")
        else:
            backup_dir.mkdir(parents=True, exist_ok=True)
        sync_once(
            plan.source, backup_dir, plan.extensions, plan.exclude_globs,
            dry_run=dry_run, desc="Backing up files"
        )

    # Main sync pass
    sync_once(
        plan.source, plan.destination, plan.extensions, plan.exclude_globs,
        dry_run=dry_run, desc="Syncing files"
    )

    if dry_run:
        print(f"[DRY-RUN] Completed planned sync: '{plan.source}' -> '{plan.destination}'")
    else:
        print(f"Synced '{plan.source}' -> '{plan.destination}'")


# ---------------------------------- CLI / INI -------------------------------- #

def load_plans_from_ini(ini_path: Path) -> Tuple[Path, List[SyncPlan]]:
    """
    @brief Load sync plans from an INI file.
    @param ini_path Path to the INI configuration file.
    @return (backup_base_dir, list_of_plans)
    @throws SystemExit on unrecoverable config errors.
    """
    cfg = configparser.ConfigParser()
    read_ok = cfg.read(ini_path)
    if not read_ok:
        raise SystemExit(f"[ERROR] Failed to read config file: {ini_path}")

    # Settings section (required: backup_dir)
    if "Settings" not in cfg:
        raise SystemExit("[ERROR] Missing [Settings] section in config.")
    backup_dir_str = cfg.get("Settings", "backup_dir", fallback="").strip()
    if not backup_dir_str:
        raise SystemExit("[ERROR] 'backup_dir' not specified in [Settings].")
    backup_base = safe_resolve(Path(backup_dir_str))

    plans: List[SyncPlan] = []
    for section in cfg.sections():
        if section == "Settings":
            continue

        src_str = cfg[section].get("source", "").strip()
        dst_str = cfg[section].get("destination", "").strip()
        if not src_str or not dst_str:
            print(f"[WARN] Skipping '{section}': missing source or destination.")
            continue

        exts = parse_csv(cfg[section].get("extensions", ""))
        exts = normalize_extensions(exts)

        backup = cfg[section].getboolean("backup", fallback=True)

        exclude_globs = parse_csv(cfg[section].get("exclude_globs", ""))

        plans.append(
            SyncPlan(
                section_name=section,
                source=safe_resolve(Path(src_str)),
                destination=safe_resolve(Path(dst_str)),
                extensions=exts,
                backup_enabled=backup,
                exclude_globs=exclude_globs,
            )
        )

    if not plans:
        print("[WARN] No sync sections found in config.")
    return backup_base, plans


def build_arg_parser() -> argparse.ArgumentParser:
    """
    @brief Build the CLI argument parser.
    """
    p = argparse.ArgumentParser(description="Directory Sync Script (pathlib edition)")
    p.add_argument(
        "-c", "--config",
        default="config.ini",
        help="Path to config.ini (default: ./config.ini)"
    )
    p.add_argument(
        "-d", "--dry-run",
        action="store_true",
        help="Simulate actions without making changes"
    )
    return p


def main() -> None:
    """
    @brief Entry point. Parses CLI, loads plans, and executes them.
    """
    args = build_arg_parser().parse_args()

    ini_path = safe_resolve(Path(args.config))
    if not ini_path.exists():
        raise SystemExit(f"[ERROR] Config file not found: {ini_path}")

    backup_base, plans = load_plans_from_ini(ini_path)

    # Ensure the backup base exists (unless dry-run; then just announce)
    if args.dry_run:
        print(f"[DRY-RUN] Would ensure backup base directory: '{backup_base}'")
    else:
        backup_base.mkdir(parents=True, exist_ok=True)

    for plan in plans:
        run_sync_job(plan, backup_base, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
