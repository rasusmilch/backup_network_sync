# backup_network_sync.py
# ===================
# Python script for synchronizing directories with backup and dry-run functionality.

# This script synchronizes files between a source and a network directory, moving files from the 
# source to a date-stamped backup location and then copying files from the network to the source. 
# The configuration of source and network directories, as well as the backup base directory, is 
# provided through a `config.ini` file.

# If specified, the script operates in a "dry-run" mode, which outputs the operations that would 
# be performed without making any actual changes. This is useful for verifying the sync actions 
# without modifying files.

# Usage:
# ------
#     python sync_directories.py --config path/to/config.ini --dry-run
#     python sync_directories.py --config path/to/config.ini

# If the --config argument is not specified, the script defaults to reading from `config.ini` 
# in the current directory.

# Command-line arguments:
# -----------------------
# - `--config`: Path to the configuration file (default: ./config.ini).
# - `--dry-run`: Optional flag to simulate actions without making changes.

# Configuration file format (`config.ini`):
# ----------------------------------------
# The configuration file should define a `[Settings]` section for the `backup_dir` and additional 
# sections for each source and network directory pair to sync.
 
# Example `config.ini`:
# ---------------------
# [Settings]
# backup_dir = C:\BackupPath
 
# [DirectoryPair1]
# source = C:\Users\Public\Documents\CEETIS\TestEndPrograms
# network = O:\Test Eng\434\TEST LAB\TestEndPrograms
 
# [DirectoryPair2]
# source = C:\Path\To\AnotherSource
# network = O:\Another\Network\Path

# Dependencies:
# -------------
# Python standard libraries:
# - configparser
# - os
# - shutil
# - datetime
# - argparse

import configparser
import os
import shutil
import datetime
import argparse

def sync_directories(source, network, backup_base_dir, dry_run):
    # Check if source directory exists
    if not os.path.exists(source):
        print(f"Source directory '{source}' does not exist. Skipping.")
        return

    # Check if network directory is accessible
    if not os.path.exists(network):
        print(f"Network directory '{network}' is not accessible. Skipping.")
        return

    # Generate date-based backup directory
    date_dir = datetime.datetime.now().strftime('%Y-%m-%d')
    relative_path = os.path.relpath(source, "C:\\")  # Exclude drive letter
    backup_dir = os.path.join(backup_base_dir, date_dir, relative_path)

    # Create backup directory
    if dry_run:
        print(f"[DRY-RUN] Would create backup directory '{backup_dir}'")
    else:
        print(f"Creating backup directory '{backup_dir}'")
        os.makedirs(backup_dir, exist_ok=True)

    # Move files from source to backup directory
    for root, _, files in os.walk(source):
        for file in files:
            src_file = os.path.join(root, file)
            relative_file_path = os.path.relpath(src_file, source)
            backup_file = os.path.join(backup_dir, relative_file_path)
            
            if dry_run:
                print(f"[DRY-RUN] Would move file '{src_file}' to '{backup_file}'")
            else:
                os.makedirs(os.path.dirname(backup_file), exist_ok=True)
                print(f"Moving file '{src_file}' to '{backup_file}'")
                shutil.copy2(src_file, backup_file)

    # Copy files from network to source directory
    for root, _, files in os.walk(network):
        for file in files:
            network_file = os.path.join(root, file)
            relative_file_path = os.path.relpath(network_file, network)
            dest_file = os.path.join(source, relative_file_path)

            if dry_run:
                print(f"[DRY-RUN] Would copy file '{network_file}' to '{dest_file}'")
            else:
                os.makedirs(os.path.dirname(dest_file), exist_ok=True)
                print(f"Copying file '{network_file}' to '{dest_file}'")
                shutil.copy2(network_file, dest_file)

    if dry_run:
        print(f"[DRY RUN] Synced '{source}' with '{network}'")
    else:
        print(f"Synced '{source}' with '{network}'")

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Directory Sync Script")
    parser.add_argument("--config", help="Path to config.ini file (default: ./config.ini)")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the actions without making changes")
    args = parser.parse_args()

    # Load configuration file, defaulting to ./config.ini if not specified
    config_path = args.config if args.config else "config.ini"
    if not os.path.exists(config_path):
        print(f"Config file '{config_path}' not found. Exiting.")
        return

    # Load configuration
    config = configparser.ConfigParser()
    config.read(config_path)

    # Get backup directory from [Settings] section
    backup_base_dir = config.get("Settings", "backup_dir", fallback=None)
    if not backup_base_dir:
        print("Backup directory not specified in [Settings]. Exiting.")
        return

    # Process each directory pair in config
    for section in config.sections():
        if section == "Settings":
            continue

        source = config[section].get("source")
        network = config[section].get("network")

        if source and network:
            print(f"\nProcessing section: {section}")
            sync_directories(source, network, backup_base_dir, args.dry_run)
        else:
            print(f"Skipping section '{section}': missing source or network path")

if __name__ == "__main__":
    main()
