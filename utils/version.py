# utils/version.py
"""
Version detection utilities for GRCAI.

Implements fallback chain:
1. __version__.py (primary)
2. setup.py / pyproject.toml metadata
3. git describe --tags
4. hardcoded "0.0.0-dev"
"""
import os
import subprocess
from pathlib import Path
from typing import Optional


def get_version() -> str:
    """
    Get GRCAI version using fallback chain.
    
    Returns:
        Version string (e.g., "1.2.3" or "0.0.0-dev")
    """
    # 1. Try __version__.py (primary)
    try:
        from __version__ import __version__
        if __version__:
            return __version__
    except ImportError:
        pass
    
    # 2. Try setup.py / pyproject.toml metadata
    try:
        import importlib.metadata
        try:
            # Try to get from installed package
            version = importlib.metadata.version("grcai")
            if version:
                return version
        except importlib.metadata.PackageNotFoundError:
            pass
        
        # Try reading pyproject.toml directly
        pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
        if pyproject_path.exists():
            try:
                # Python 3.11+ has tomllib
                import tomllib
                with open(pyproject_path, "rb") as f:
                    data = tomllib.load(f)
                    if "project" in data and "version" in data["project"]:
                        return data["project"]["version"]
            except ImportError:
                # Python < 3.11 - try tomli as fallback
                try:
                    import tomli
                    with open(pyproject_path, "rb") as f:
                        data = tomli.load(f)
                        if "project" in data and "version" in data["project"]:
                            return data["project"]["version"]
                except ImportError:
                    pass
    except Exception:
        pass
    
    # 3. Try git describe --tags
    try:
        repo_root = Path(__file__).parent.parent
        result = subprocess.run(
            ["git", "describe", "--tags", "--always"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=2
        )
        if result.returncode == 0 and result.stdout.strip():
            version = result.stdout.strip()
            # Remove 'v' prefix if present
            if version.startswith("v"):
                version = version[1:]
            return version
    except Exception:
        pass
    
    # 4. Fallback
    return "0.0.0-dev"


def get_build() -> str:
    """
    Get build identifier (git commit SHA or tag).
    
    Returns:
        Git commit SHA (short), tag, or "unknown"
    """
    try:
        repo_root = Path(__file__).parent.parent
        
        # Try git describe first (tags)
        result = subprocess.run(
            ["git", "describe", "--tags", "--always"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=2
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        
        # Fallback to git rev-parse (commit SHA)
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=2
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    
    return "unknown"


def get_contracts_path() -> str:
    """
    Determine contracts directory path.

    Preference:
    1) CONTRACTS_PATH env var (dev mounts / image path)
    2) repo-local ./contracts (copied contracts)
    """
    return os.getenv("CONTRACTS_PATH") or str(Path(__file__).resolve().parents[1] / "contracts")


def get_contracts_version() -> str:
    """
    Read contracts SemVer from <contracts_path>/version.txt.
    Returns "unknown" if missing/unreadable.
    """
    try:
        p = Path(get_contracts_path()) / "version.txt"
        return p.read_text(encoding="utf-8").strip() or "unknown"
    except Exception:
        return "unknown"


def semver_major(version: str) -> Optional[int]:
    """
    Parse MAJOR from a SemVer-ish string like '1.2.3' or 'v1.2.3'.
    Returns None if parsing fails.
    """
    if not version:
        return None
    v = version.strip()
    if v.startswith("v"):
        v = v[1:]
    major_str = v.split(".", 1)[0]
    try:
        return int(major_str)
    except Exception:
        return None

