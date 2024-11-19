import configparser
import os
import shutil
import datetime
import argparse
from tqdm import tqdm
import sys 

def should_update_file(src_entry, dest_file):
    """
    Determines if the source file should be copied to the destination
    based on modification times and file sizes.
    """
    try:
        if not os.path.exists(dest_file):
            return True  # Destination file doesn't exist, so it needs to be copied

        # Get metadata for source and destination
        src_stat = src_entry.stat()  # Efficiently retrieves metadata during directory scan
        dest_stat = os.stat(dest_file)

        # Compare modification times and file sizes
        return src_stat.st_mtime > dest_stat.st_mtime or src_stat.st_size != dest_stat.st_size
    except FileNotFoundError:
        return True  # If destination file is missing

def count_files(directory, extensions=None):
    """
    Count the total number of files in a directory tree, optionally filtered by extensions.
    
    :param directory: The directory to count files in.
    :param extensions: List of allowed file extensions (e.g., ['.txt', '.log']). Count all if None or ["*"].
    :return: Total file count.
    """
    total_files = 0
    for entry in os.scandir(directory):
        if entry.is_dir():
            # Recursively count files in subdirectories
            total_files += count_files(entry.path, extensions)
        elif entry.is_file():
            # Check file extensions, if provided
            if extensions and extensions != ["*"] and not entry.name.endswith(tuple(extensions)):
                continue
            total_files += 1
    return total_files

def sync_directories_recursive(source, destination, extensions, dry_run, progress_bar=None):
    """
    Recursively syncs directories using os.scandir.
    """

    for entry in os.scandir(source):
        source_path = entry.path
        relative_path = os.path.relpath(source_path, source)
        destination_path = os.path.join(destination, relative_path)

        if entry.is_dir():
            # Handle subdirectories
            # print(f"Processing directory: {source_path}...")

            if not os.path.exists(destination_path):
                if dry_run:
                    print(f"[DRY-RUN] Would create directory '{destination_path}'")
                else:
                    os.makedirs(destination_path, exist_ok=True)
                    # print(f"Created directory '{destination_path}'")
            # Recursive call for subdirectory
            sync_directories_recursive(source_path, destination_path, extensions, dry_run, progress_bar)
        elif entry.is_file():
            # Handle files
            
            if extensions and extensions != ["*"] and not entry.name.endswith(tuple(extensions)):
                # print(f"{entry.name} excluded")
                continue
            if should_update_file(entry, destination_path):
                if dry_run:
                    print(f"[DRY-RUN] Would copy file '{source_path}' to '{destination_path}'")
                else:
                    os.makedirs(os.path.dirname(destination_path), exist_ok=True)
                    # print(f"Copying file '{source_path}' to '{destination_path}'")
                    shutil.copy2(source_path, destination_path)
            
            # print(f"Processing file [{source_path}]")
            # Update progress bar if provided
            if progress_bar:
                # print(f"Updating progress bar")
                progress_bar.update(1)

def sync_directories(source, destination, backup_base_dir, extensions, backup, dry_run):
    # Normalize extensions
    extensions = normalize_extensions(extensions)

    # Check if source directory exists
    if not os.path.exists(source):
        print(f"Source directory '{source}' does not exist. Skipping.")
        return

    # Check if destination directory is accessible
    if not os.path.exists(destination):
        print(f"Destination directory '{destination}' does not exist. Skipping.")
        return

    # Count files and initialize progress bar
    total_files = count_files(source, extensions)
    
    # Perform backup if the backup flag is set to true
    if backup:
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
        with tqdm(total=total_files, desc="Backing up files", dynamic_ncols=True, disable=not sys.stdout.isatty()) as progress_bar:
            sync_directories_recursive(source, backup_dir, extensions, dry_run, progress_bar)



    # Sync files from source to destination directory
    with tqdm(total=total_files, desc="Syncing files", dynamic_ncols=True, disable=not sys.stdout.isatty()) as progress_bar:
        sync_directories_recursive(source, destination, extensions, dry_run, progress_bar)

    if dry_run:
        print(f"[DRY RUN] Synced '{source}' with '{destination}'")
    else:
        print(f"Synced '{source}' with '{destination}'")

def normalize_extensions(extensions):
    """
    Normalize extensions to ensure they start with a dot.
    """
    if not extensions:
        return []
    return [ext if ext.startswith(".") else f".{ext}" for ext in extensions]

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Directory Sync Script")
    parser.add_argument("-c", "--config", required=True, help="Path to config.ini file (default: ./config.ini)")
    parser.add_argument("-d", "--dry-run", action="store_true", help="Simulate the actions without making changes")
    args = parser.parse_args()

    # Load configuration file, defaulting to ./config.ini if not specified
    config_path = args.config
    
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
        destination = config[section].get("destination")
        extensions = config[section].get("extensions", "").split(",")  # Get extensions as a list
        backup = config[section].getboolean("backup", fallback=True)  # Default to true if not specified

        if source and destination:
            print(f"\nProcessing section: {section}")
            sync_directories(source, destination, backup_base_dir, extensions, backup, args.dry_run)
        else:
            print(f"Skipping section '{section}': missing source or destination path")

if __name__ == "__main__":
    main()
