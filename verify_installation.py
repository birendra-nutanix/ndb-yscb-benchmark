#!/usr/bin/env python3
"""
Simple verification script to check installation
Run this after installing dependencies: pip install -r requirements.txt
"""

import sys
from pathlib import Path

def verify_structure():
    """Verify project structure"""
    print("Verifying project structure...")
    
    base_dir = Path(__file__).parent
    required_files = [
        "app.py",
        "requirements.txt",
        "README.md",
        "QUICKSTART.md",
        "USAGE_EXAMPLES.md",
        "run.sh",
        "static/css/styles.css",
        "static/js/app.js",
        "templates/index.html",
        "generators/__init__.py",
        "generators/script_generator.py",
        "generators/ycsb_config.py",
        "validators/__init__.py",
        "validators/ndb_validator.py",
    ]
    
    missing_files = []
    for file_path in required_files:
        full_path = base_dir / file_path
        if not full_path.exists():
            missing_files.append(file_path)
    
    if missing_files:
        print("✗ Missing files:")
        for file in missing_files:
            print(f"  - {file}")
        return False
    else:
        print("✓ All required files present")
        return True

def verify_dependencies():
    """Verify Python dependencies"""
    print("\nVerifying dependencies...")
    
    required_modules = [
        ("fastapi", "FastAPI"),
        ("uvicorn", "Uvicorn"),
        ("jinja2", "Jinja2"),
        ("pydantic", "Pydantic"),
        ("requests", "Requests"),
    ]
    
    missing_modules = []
    for module_name, display_name in required_modules:
        try:
            __import__(module_name)
            print(f"✓ {display_name}")
        except ImportError:
            missing_modules.append(display_name)
            print(f"✗ {display_name}")
    
    if missing_modules:
        print("\nMissing dependencies. Install with:")
        print("  pip install -r requirements.txt")
        return False
    
    return True

def main():
    print("=" * 80)
    print("NDB YCSB Benchmark Generator - Installation Verification")
    print("=" * 80)
    print()
    
    structure_ok = verify_structure()
    deps_ok = verify_dependencies()
    
    print()
    print("=" * 80)
    
    if structure_ok and deps_ok:
        print("✓ Installation verified successfully!")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Start the application:")
        print("     ./run.sh")
        print("     OR")
        print("     python app.py")
        print()
        print("  2. Access the UI:")
        print("     http://localhost:8000")
        print()
        print("  3. Read the documentation:")
        print("     - README.md (full documentation)")
        print("     - QUICKSTART.md (quick start guide)")
        print("     - USAGE_EXAMPLES.md (usage examples)")
        return 0
    else:
        print("✗ Installation verification failed")
        print("=" * 80)
        print()
        if not structure_ok:
            print("Some files are missing. Please ensure all files are present.")
        if not deps_ok:
            print("Dependencies not installed. Run: pip install -r requirements.txt")
        return 1

if __name__ == "__main__":
    sys.exit(main())
