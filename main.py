import os

import sys
from typing import List, Set, Tuple
from datetime import datetime
from thefuzz import fuzz
import pandas as pd

def should_ignore(path: str, name: str, ignore_paths: Set[str], ignore_exts: Set[str], ignore_names: Set[str], ignore_hidden: bool, allowed_exts: Set[str]) -> bool:
    # Check allowed extensions
    if allowed_exts:
        _, ext = os.path.splitext(name)
        if ext.lower() not in allowed_exts:
            return True

    # Check hidden
    if ignore_hidden and name.startswith('.'):
        return True

    # Check filename/directory name exactly
    if name in ignore_names or name in ignore_paths:
        return True
    
    # Check extension
    _, ext = os.path.splitext(name)
    if ext.lower() in ignore_exts:
        return True
    
    # Check full path components allows ignoring "node_modules" specifically everywhere
    path_parts = path.split(os.sep)
    for part in path_parts:
        if part in ignore_paths:
            return True
        if ignore_hidden and part.startswith('.'):
             return True
             
    # Check relative path matching (e.g. "src/legacy")
    for ign in ignore_paths:
        # Normalize ignore path to system separator
        norm_ign = os.path.normpath(ign)
        if norm_ign in path: 
            # Check boundaries to avoid partial name match (e.g. ignore "bin" matching "cabin")
            if f"{os.sep}{norm_ign}{os.sep}" in f"{os.sep}{path}{os.sep}":
                return True
            
    return False

def get_mod_time(path: str) -> datetime:
    try:
        ts = os.path.getmtime(path)
        return datetime.fromtimestamp(ts)
    except Exception:
        return datetime.min

def normalize_for_match(filename: str) -> str:
    """Removes extension and converts to lowercase for better matching."""
    return os.path.splitext(filename)[0].lower()

def group_files(file_list: List[Tuple[str, datetime]], threshold: int = 80) -> List[List[Tuple[str, datetime]]]:
    """
    Groups files based on fuzzy matching of their names.
    file_list is a list of (path, mod_time_dt) tuples.
    Returns a list of groups.
    """
    groups = []
    
    # Sort by path for consistent iteration
    sorted_files = sorted(file_list, key=lambda x: x[0])
    
    assigned_indices = set()
    
    for i in range(len(sorted_files)):
        if i in assigned_indices:
            continue
            
        current_file = sorted_files[i]
        current_name = os.path.basename(current_file[0])
        norm_name = normalize_for_match(current_name)
        
        # Start a new group
        current_group = [current_file]
        assigned_indices.add(i)
        
        # Look for matches in the REST of the list
        for j in range(i + 1, len(sorted_files)):
            if j in assigned_indices:
                continue
                
            other_file = sorted_files[j]
            other_name = os.path.basename(other_file[0])
            norm_other = normalize_for_match(other_name)
            
            # Simple exact match of normalized name (covers extensions case)
            if norm_name == norm_other:
                current_group.append(other_file)
                assigned_indices.add(j)
                continue
                
            # Fuzzy match
            score = fuzz.ratio(norm_name, norm_other)
            if score >= threshold:
                current_group.append(other_file)
                assigned_indices.add(j)
                
        groups.append(current_group)
        
    return groups

def export_to_excel(groups: List[List[Tuple[str, datetime]]], output_file: str):
    """
    Exports the grouped files to an Excel file with specific columns.
    """
    data = []
    for group in groups:
        # Determine group properties
        
        # 1. All File Names
        all_names = [os.path.basename(f[0]) for f in group]
        all_names_str = ", ".join(all_names)
        
        # 2. PDF / Word Exists
        pdf_exists = any(f[0].lower().endswith('.pdf') for f in group)
        word_exists = any(f[0].lower().endswith(('.doc', '.docx')) for f in group)
        
        # 3. Last Modified (Max of group)
        last_modified = max((f[1] for f in group), default=datetime.min)
        
        # 4. File Name (Representative)
        # We can use the name of the most recently modified file, without extension
        most_recent_file = max(group, key=lambda x: x[1])
        base_name = os.path.splitext(os.path.basename(most_recent_file[0]))[0]
        
        data.append({
            "File Name": base_name,
            "PDF Exists": "Yes" if pdf_exists else "No",
            "Word Doc Exists": "Yes" if word_exists else "No",
            "Last Modified": last_modified,
            "All File Names": all_names_str
        })
        
    df = pd.DataFrame(data)
    
    # Format date column if not empty
    if not df.empty and pd.api.types.is_datetime64_any_dtype(df["Last Modified"]):
        df["Last Modified"] = df["Last Modified"].dt.strftime('%d/%m/%Y %H:%M:%S')
    elif not df.empty:
        # Fallback if somehow not datetime (shouldn't happen with default=min)
        pass
        
    try:
        df.to_excel(output_file, index=False)
        print(f"Excel report saved to: {output_file}")
    except Exception as e:
        print(f"Error saving Excel report: {e}")

def scan_files(paths: List[str], ignore_paths: List[str] = None, ignore_exts: List[str] = None, ignore_names: List[str] = None, ignore_hidden: bool = True, allowed_exts: List[str] = None, output_file: str = "scan_results.xlsx", sheet_names: List[str] = None) -> None:
    """
    Scans, groups, and exports file data.
    Creates a separate sheet for each path.
    """
    if ignore_paths is None: ignore_paths = []
    if ignore_exts is None: ignore_exts = []
    if ignore_names is None: ignore_names = []
    if allowed_exts is None: allowed_exts = []
    if sheet_names is None: sheet_names = []

    # Convert to sets for efficient lookup and normalize
    ign_paths_set = set(ignore_paths)
    ign_exts_set = {e if e.startswith('.') else f'.{e}' for e in ignore_exts}
    ign_exts_set = {e.lower() for e in ign_exts_set}
    ign_names_set = set(ignore_names)
    
    allowed_exts_set = {e if e.startswith('.') else f'.{e}' for e in allowed_exts}
    allowed_exts_set = {e.lower() for e in allowed_exts_set}
    
    print(f"Scanning Paths: {paths}")
    print(f"Ignoring Paths: {ign_paths_set}")
    print("-" * 40)

    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            sheets_written = 0
            
            for i, target_path in enumerate(paths):
                # Skip empty paths
                if not target_path or not target_path.strip():
                    print(f"Skipping empty path at index {i}")
                    continue
                
                print(f"Processing path: {target_path}")
                
                # Get sheet name for this path
                if i < len(sheet_names):
                    sheet_name = sheet_names[i]
                else:
                    # Fallback to path basename if no sheet name provided
                    if target_path == ".":
                        sheet_name = "Root"
                    else:
                        sheet_name = os.path.basename(os.path.abspath(target_path))
                        if not sheet_name:
                            sheet_name = "Root"
                
                # Limit sheet name to 31 characters (Excel limit)
                sheet_name = sheet_name[:31]
                
                all_collected_files = []
                target_path_abs = os.path.abspath(target_path)
                
                if not os.path.exists(target_path_abs):
                    print(f"  Warning: Path '{target_path_abs}' does not exist. Skipping.")
                    continue
                    
                if os.path.isfile(target_path_abs):
                    name = os.path.basename(target_path_abs)
                    if not should_ignore(target_path_abs, name, ign_paths_set, ign_exts_set, ign_names_set, ignore_hidden, allowed_exts_set):
                        try:
                            rel_p = os.path.relpath(target_path_abs)
                        except ValueError:
                            rel_p = target_path_abs
                        mod_time = get_mod_time(target_path_abs)
                        all_collected_files.append((rel_p, mod_time))
                else:
                    for root, dirs, files in os.walk(target_path_abs):
                        dirs_to_remove = []
                        for d in dirs:
                            full_dir_path = os.path.join(root, d)
                            if should_ignore(full_dir_path, d, ign_paths_set, ign_exts_set, ign_names_set, ignore_hidden, allowed_exts=None): 
                                 dirs_to_remove.append(d)
                            
                        for d in dirs_to_remove:
                            dirs.remove(d)

                        for file in files:
                            full_path = os.path.join(root, file)
                            
                            if should_ignore(full_path, file, ign_paths_set, ign_exts_set, ign_names_set, ignore_hidden, allowed_exts_set):
                                continue
                                
                            try:
                                rel_path = os.path.relpath(full_path)
                            except ValueError:
                                rel_path = full_path
                            
                            mod_time = get_mod_time(full_path)
                            all_collected_files.append((rel_path, mod_time))

                # Group files for this path
                print(f"  Found {len(all_collected_files)} files. Grouping...")
                grouped_files = group_files(all_collected_files, threshold=80)
                
                # Create DataFrame for this sheet
                data = []
                for group in grouped_files:
                    all_names = [os.path.basename(f[0]) for f in group]
                    all_names_str = ", ".join(all_names)
                    
                    pdf_exists = any(f[0].lower().endswith('.pdf') for f in group)
                    word_exists = any(f[0].lower().endswith(('.doc', '.docx')) for f in group)
                    
                    last_modified = max((f[1] for f in group), default=datetime.min)
                    
                    most_recent_file = max(group, key=lambda x: x[1])
                    base_name = os.path.splitext(os.path.basename(most_recent_file[0]))[0]
                    
                    data.append({
                        "File Name": base_name,
                        "PDF Exists": "Yes" if pdf_exists else "No",
                        "Word Doc Exists": "Yes" if word_exists else "No",
                        "Last Modified": last_modified,
                        "All File Names": all_names_str
                    })
                
                df = pd.DataFrame(data)
                
                # Format date column if not empty
                if not df.empty and pd.api.types.is_datetime64_any_dtype(df["Last Modified"]):
                    df["Last Modified"] = df["Last Modified"].dt.strftime('%d/%m/%Y %H:%M:%S')
                
                # Write to sheet
                try:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    sheets_written += 1
                    print(f"  Written sheet: {sheet_name}")
                except ValueError:
                    # Handle duplicate sheet names
                    final_sheet_name = f"{sheet_name}_{i}"
                    df.to_excel(writer, sheet_name=final_sheet_name, index=False)
                    sheets_written += 1
                    print(f"  Written sheet: {final_sheet_name}")
            
            print("-" * 40)
            if sheets_written > 0:
                print(f"Excel report saved to: {output_file} ({sheets_written} sheets)")
            else:
                print("No sheets written (no paths found?)")
                
    except Exception as e:
        print(f"Error saving Excel report: {e}")
        import traceback
        traceback.print_exc()

def parse_scan_paths_file(file_path: str) -> Tuple[List[str], List[str]]:
    """
    Parses a text file with hashtag department names and paths.
    Format:
        #DE
        C:\\Users\\vidr8fu\\OneDrive - VicGov\\Budget Automatio\\Budget Hub - DE
        #DEECA
        C:\\Users\\vidr8fu\\OneDrive - VicGov\\Budget Automatio\\Budget Hub - DEECA
    
    Returns:
        Tuple of (paths list, sheet_names list)
    """
    paths = []
    sheet_names = []
    
    if not os.path.exists(file_path):
        print(f"Warning: Scan paths file '{file_path}' does not exist.")
        return paths, sheet_names
    
    try:
        # Try UTF-8 first, then fall back to other encodings
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
        file_content = None
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                    file_content = f.readlines()
                    break
            except UnicodeDecodeError:
                continue
        
        if file_content is None:
            # Last resort: read as binary and decode with errors='replace'
            with open(file_path, 'rb') as f:
                file_content = [line.decode('utf-8', errors='replace') for line in f]
        
        current_dept = None
        for line in file_content:
            line = line.strip()
            if not line:  # Skip empty lines
                continue
            
            if line.startswith('#'):
                # This is a department name
                # Reset any previous department that didn't have a path
                current_dept = line[1:].strip()  # Remove the '#' and whitespace
            elif current_dept:
                # This is a path for the current department
                if line:  # Only add non-empty paths
                    paths.append(line)
                    sheet_names.append(current_dept)
                    current_dept = None  # Reset for next department
            else:
                # Path without a department name - use path basename as sheet name
                if line:
                    paths.append(line)
                    # Use basename as sheet name, or "Root" if empty
                    basename = os.path.basename(os.path.abspath(line))
                    sheet_names.append(basename if basename else "Root")
    except Exception as e:
        print(f"Error reading scan paths file '{file_path}': {e}")
        import traceback
        traceback.print_exc()
    
    return paths, sheet_names

def parse_config_file(file_path: str) -> dict:
    """
    Parses a configuration text file with key-value pairs.
    Format:
        IGNORE_PATHS=path1,path2,path3
        IGNORE_EXTS=.pyc,.ds_store
        IGNORE_NAMES=
        IGNORE_HIDDEN=True
        ALLOWED_EXTS=.docx,.pdf
        OUTPUT_FILE=grouping_report.xlsx
    
    Returns:
        Dictionary with configuration values
    """
    config = {
        'ignore_paths': [],
        'ignore_exts': [],
        'ignore_names': [],
        'ignore_hidden': True,
        'allowed_exts': [],
        'output_file': 'grouping_report.xlsx'
    }
    
    if not os.path.exists(file_path):
        print(f"Warning: Config file '{file_path}' does not exist. Using defaults.")
        return config
    
    try:
        # Try UTF-8 first, then fall back to other encodings
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
        file_content = None
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                    file_content = f.readlines()
                    break
            except UnicodeDecodeError:
                continue
        
        if file_content is None:
            # Last resort: read as binary and decode with errors='replace'
            with open(file_path, 'rb') as f:
                file_content = [line.decode('utf-8', errors='replace') for line in f]
        
        for line in file_content:
            line = line.strip()
            if not line or line.startswith('#'):  # Skip empty lines and comments
                continue
            
            if '=' not in line:
                continue
            
            key, value = line.split('=', 1)
            key = key.strip().upper()
            value = value.strip()
            
            if key == 'IGNORE_PATHS':
                config['ignore_paths'] = [p.strip() for p in value.split(',') if p.strip()]
            elif key == 'IGNORE_EXTS':
                config['ignore_exts'] = [e.strip() for e in value.split(',') if e.strip()]
            elif key == 'IGNORE_NAMES':
                config['ignore_names'] = [n.strip() for n in value.split(',') if n.strip()]
            elif key == 'IGNORE_HIDDEN':
                config['ignore_hidden'] = value.lower() in ('true', '1', 'yes', 'on')
            elif key == 'ALLOWED_EXTS':
                config['allowed_exts'] = [e.strip() for e in value.split(',') if e.strip()]
            elif key == 'OUTPUT_FILE':
                config['output_file'] = value
    except Exception as e:
        print(f"Error reading config file '{file_path}': {e}")
        import traceback
        traceback.print_exc()
    
    return config

def parse_ignore_paths_file(file_path: str) -> List[str]:
    """
    Parses a text file with file paths to exclude, one per line.
    Format:
        C:\\Users\\vidr8fu\\OneDrive - VicGov\\Budget Automatio\\Budget Hub - DE\\Costings
        C:\\Users\\vidr8fu\\OneDrive - VicGov\\Budget Automatio\\Budget Hub - CSV\\Costings
    
    Returns:
        List of paths to ignore
    """
    paths = []
    
    if not os.path.exists(file_path):
        print(f"Warning: Ignore paths file '{file_path}' does not exist.")
        return paths
    
    try:
        # Try UTF-8 first, then fall back to other encodings
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
        file_content = None
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                    file_content = f.readlines()
                    break
            except UnicodeDecodeError:
                continue
        
        if file_content is None:
            # Last resort: read as binary and decode with errors='replace'
            with open(file_path, 'rb') as f:
                file_content = [line.decode('utf-8', errors='replace') for line in f]
        
        for line in file_content:
            line = line.strip()
            if line and not line.startswith('#'):  # Skip empty lines and comments
                paths.append(line)
    except Exception as e:
        print(f"Error reading ignore paths file '{file_path}': {e}")
        import traceback
        traceback.print_exc()
    
    return paths

def parse_ignore_names_file(file_path: str) -> List[str]:
    """
    Parses a text file with file names to exclude (without extensions), one per line.
    Format:
        .DS_Store
        Thumbs.db
        desktop.ini
    
    Returns:
        List of file names to ignore
    """
    names = []
    
    if not os.path.exists(file_path):
        print(f"Warning: Ignore names file '{file_path}' does not exist.")
        return names
    
    try:
        # Try UTF-8 first, then fall back to other encodings
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
        file_content = None
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                    file_content = f.readlines()
                    break
            except UnicodeDecodeError:
                continue
        
        if file_content is None:
            # Last resort: read as binary and decode with errors='replace'
            with open(file_path, 'rb') as f:
                file_content = [line.decode('utf-8', errors='replace') for line in f]
        
        for line in file_content:
            line = line.strip()
            if line and not line.startswith('#'):  # Skip empty lines and comments
                names.append(line)
    except Exception as e:
        print(f"Error reading ignore names file '{file_path}': {e}")
        import traceback
        traceback.print_exc()
    
    return names

# ==========================================
# CONFIGURATION
# ==========================================
# File paths for input configuration
SCAN_PATHS_FILE = "scan_paths.txt"  # File with hashtag department names and paths
IGNORE_PATHS_FILE = "ignore_paths.txt"  # File with paths to exclude, one per line
IGNORE_NAMES_FILE = "ignore_names.txt"  # File with file names to exclude (no extension), one per line
CONFIG_FILE = "config.txt"  # Optional file with other configuration

# Default values (used if config file doesn't exist or doesn't specify values)
DEFAULT_IGNORE_PATHS = []
DEFAULT_IGNORE_EXTS = [".pyc", ".ds_store"]
DEFAULT_IGNORE_NAMES = []
DEFAULT_IGNORE_HIDDEN = True
DEFAULT_ALLOWED_EXTS = [".docx", ".pdf"]
DEFAULT_OUTPUT_FILE = "grouping_report.xlsx"

def main():
    # Parse scan paths file
    scan_paths, sheet_names = parse_scan_paths_file(SCAN_PATHS_FILE)
    
    # Parse ignore paths file
    ignore_paths_from_file = parse_ignore_paths_file(IGNORE_PATHS_FILE)
    
    # Parse ignore names file
    ignore_names_from_file = parse_ignore_names_file(IGNORE_NAMES_FILE)
    
    # Parse config file (if exists)
    config = parse_config_file(CONFIG_FILE)
    
    # Combine ignore paths: merge from file, config, and defaults (remove duplicates)
    ignore_paths = list(set(ignore_paths_from_file + (config.get('ignore_paths') or []) + DEFAULT_IGNORE_PATHS))
    
    # Combine ignore names: merge from file, config, and defaults (remove duplicates)
    ignore_names = list(set(ignore_names_from_file + (config.get('ignore_names') or []) + DEFAULT_IGNORE_NAMES))
    
    # Use config file values if available, otherwise use defaults
    ignore_exts = config.get('ignore_exts') or DEFAULT_IGNORE_EXTS
    ignore_hidden = config.get('ignore_hidden', DEFAULT_IGNORE_HIDDEN)
    allowed_exts = config.get('allowed_exts') or DEFAULT_ALLOWED_EXTS
    output_file = config.get('output_file') or DEFAULT_OUTPUT_FILE
    
    # If no paths were found from file, show a message
    if not scan_paths:
        print(f"No scan paths found in '{SCAN_PATHS_FILE}'. Please create the file with format:")
        print("#DE")
        print(r"C:\Users\vidr8fu\OneDrive - VicGov\Budget Automatio\Budget Hub - DE")
        print("#DEECA")
        print(r"C:\Users\vidr8fu\OneDrive - VicGov\Budget Automatio\Budget Hub - DEECA")
        return
    
    scan_files(
        paths=scan_paths,
        ignore_paths=ignore_paths,
        ignore_exts=ignore_exts,
        ignore_names=ignore_names,
        ignore_hidden=ignore_hidden,
        allowed_exts=allowed_exts,
        output_file=output_file,
        sheet_names=sheet_names
    )

if __name__ == "__main__":
    main()

print("sync test from antigravity")
print("sync test from antigravity")
print("sync test from antigravity")
print("sync test from antigravity")
print("sync test from antigravity")
