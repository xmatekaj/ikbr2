#!/usr/bin/env python3
"""
Simple Directory Structure Analyzer
----------------------------------
Scans a directory and creates a text file that contains:
1. A list of all file types in the directory
2. The directory structure with files and subdirectories
3. The content of each Python, text, and JSON file
"""

import os
import sys
from pathlib import Path

def is_excluded_path(path, exclude_dirs):
    """
    Check if any part of the path contains an excluded directory name.
    """
    path_parts = Path(path).parts
    return any(excluded in path_parts for excluded in exclude_dirs)

def main():
    # Get the directory to analyze (current directory if not specified)
    if len(sys.argv) > 1:
        root_dir = sys.argv[1]
    else:
        root_dir = os.getcwd()
    
    # Get absolute paths
    root_path = Path(root_dir).resolve()
    current_script_path = Path(__file__).resolve()
    output_path = root_path / "output.txt"
    
    # File extensions to include
    include_types = ['.py', '.txt', '.json']
    
    # Directories to exclude
    exclude_dirs = ['.git', '__pycache__', 'node_modules', '.idea', '.vscode', 'venv']
    
    # Collect all files and extensions
    all_files = []
    all_extensions = set()
    
    # Start writing output
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(f"DIRECTORY STRUCTURE ANALYSIS\n")
        f.write(f"===========================\n")
        f.write(f"Root Directory: {root_path}\n\n")
        
        # Walk through directory
        for current_dir, dirs, files in os.walk(root_path):
            # Skip if this directory or any parent is in exclude_dirs
            if is_excluded_path(current_dir, exclude_dirs):
                dirs[:] = []  # Don't descend into this directory
                continue
            
            # Skip excluded directories for next iteration
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            
            # Get all file extensions (excluding excluded paths)
            for file in files:
                file_path = os.path.join(current_dir, file)
                if is_excluded_path(file_path, exclude_dirs):
                    continue
                    
                _, ext = os.path.splitext(file)
                if ext:
                    all_extensions.add(ext)
            
            # Calculate relative path for indentation
            rel_path = os.path.relpath(current_dir, root_path)
            indent = '  ' * (rel_path.count(os.sep))
            
            # Write directory name
            if rel_path == '.':
                f.write(f"\n{os.path.basename(root_path)}/\n")
            else:
                f.write(f"{indent}{os.path.basename(current_dir)}/\n")
            
            # Process files in this directory
            for file in sorted(files):
                file_path = os.path.join(current_dir, file)
                abs_file_path = os.path.abspath(file_path)
                
                # Skip the current script
                if abs_file_path == current_script_path:
                    continue
                    
                # Skip if file is in an excluded path
                if is_excluded_path(file_path, exclude_dirs):
                    continue
                
                # Filter by extension
                _, ext = os.path.splitext(file)
                if ext.lower() in include_types:
                    all_files.append(file_path)
                    f.write(f"{indent}  {file}\n")
        
        # Write all file types found
        f.write("\nFile Types Found in Directory: ")
        f.write(", ".join(sorted(all_extensions)))
        f.write("\n\n")
        
        # Section 2: File Contents
        f.write(f"\nFILE CONTENTS\n")
        f.write(f"=============\n\n")
        
        # Process each file
        for file_path in all_files:
            rel_file_path = os.path.relpath(file_path, root_path)
            
            f.write(f"\n{'=' * 80}\n")
            f.write(f"FILE: {rel_file_path}\n")
            f.write(f"{'=' * 80}\n\n")
            
            # Try to read the file content
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file_handle:
                    content = file_handle.read()
                    f.write(content)
                    # Add a newline if the file doesn't end with one
                    if content and not content.endswith('\n'):
                        f.write('\n')
            except Exception as e:
                f.write(f"[ERROR READING FILE: {str(e)}]\n")
    
    print(f"Analysis complete. Results written to: {output_path}")

if __name__ == "__main__":
    main()