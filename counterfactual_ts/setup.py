"""
Setup and environment utilities for counterfactual_ts library.
"""

import os
import sys
import subprocess
import platform
from typing import Optional, List
from pathlib import Path


def run_setup_script(
    script_path: str,
    check: bool = True
) -> bool:
    """
    Run a setup script (bash/shell script or batch file).

    Automatically detects OS and runs appropriate script.

    Args:
        script_path: Path to setup script
        check: Whether to raise exception on failure (default: True)

    Returns:
        True if successful, False otherwise
    """
    script_path = Path(script_path)

    if not script_path.exists():
        raise FileNotFoundError(f"Setup script not found: {script_path}")

    if platform.system() == 'Windows' and script_path.suffix in ['.bat', '.cmd']:
        command = [str(script_path)]
    elif script_path.suffix == '.sh':
        command = ['bash', str(script_path)]
    else:
        command = [str(script_path)]
        # Only needed when the file is executed directly rather than handed
        # to an interpreter.
        if platform.system() != 'Windows':
            os.chmod(script_path, 0o755)

    try:
        # No shell=True here: on POSIX it would treat everything after the
        # first list element as positional args to the shell and run nothing.
        result = subprocess.run(command, check=check)
        return result.returncode == 0
    except subprocess.CalledProcessError:
        if check:
            raise
        return False
    except Exception as e:
        print(f"Error running setup script: {e}")
        return False


def find_setup_script(directory: str = '.', prefer_os_specific: bool = True) -> Optional[str]:
    """
    Find setup script in directory.
    
    Looks for: setup.sh, setup.bat, setup.cmd, setup.py
    
    Args:
        directory: Directory to search (default: current directory)
        prefer_os_specific: If True, prefer OS-specific scripts
    
    Returns:
        Path to setup script, or None if not found
    """
    directory = Path(directory)
    system = platform.system()
    
    # OS-specific scripts to look for
    if system == 'Windows':
        candidates = ['setup.bat', 'setup.cmd', 'setup.sh', 'setup.py']
    else:
        candidates = ['setup.sh', 'setup.bat', 'setup.cmd', 'setup.py']
    
    for candidate in candidates:
        script_path = directory / candidate
        if script_path.exists():
            return str(script_path)
    
    return None


def install_dependencies(
    requirements_file: str = 'requirements.txt',
    use_pip: bool = True
) -> bool:
    """
    Install dependencies from requirements file.
    
    Args:
        requirements_file: Path to requirements.txt
        use_pip: Whether to use pip (True) or conda (False)
    
    Returns:
        True if successful, False otherwise
    """
    requirements_path = Path(requirements_file)
    
    if not requirements_path.exists():
        print(f"Warning: Requirements file not found: {requirements_path}")
        return False
    
    try:
        if use_pip:
            result = subprocess.run(
                [sys.executable, '-m', 'pip', 'install', '-r', str(requirements_path)],
                check=True
            )
        else:
            result = subprocess.run(
                ['conda', 'install', '--file', str(requirements_path)],
                check=True
            )
        
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Error installing dependencies: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False


def setup_environment(
    directory: str = '.',
    run_script: bool = True,
    install_deps: bool = True,
    requirements_file: str = 'requirements.txt'
) -> dict:
    """
    Complete environment setup.

    Args:
        directory: Directory containing setup files
        run_script: Whether to run setup script if found
        install_deps: Whether to install dependencies
        requirements_file: Path to requirements file

    Returns:
        Dictionary with setup results:
            - 'setup_script_run': bool
            - 'setup_script_path': str or None
            - 'dependencies_installed': bool
            - 'success': bool
    """
    results = {
        'setup_script_run': False,
        'setup_script_path': None,
        'dependencies_installed': False,
        'success': False
    }
    
    # Find and run setup script
    if run_script:
        script_path = find_setup_script(directory)
        if script_path:
            results['setup_script_path'] = script_path
            results['setup_script_run'] = run_setup_script(script_path, check=False)

    # Install dependencies
    if install_deps:
        results['dependencies_installed'] = install_dependencies(requirements_file)

    # Overall success
    results['success'] = (
        (not run_script or results['setup_script_run']) and
        (not install_deps or results['dependencies_installed'])
    )

    return results

