"""
NX Migrator Pro
Professional partition management for Nintendo Switch SD cards
Features:
- Migration Mode: Migrate partitions from smaller SD to larger SD
- Cleanup Mode: Remove unwanted partitions and expand FAT32
Supports: FAT32, Linux (L4T), Android (Dynamic/Legacy), emuMMC (Single/Dual)
License: GPL-2.0 (same as hekate)
"""

# Version information
__version__ = "1.0.0"

import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import messagebox
import sys
import os
import logging
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from core.disk_manager import DiskManager
from core.partition_scanner import PartitionScanner
from core.migration_engine import MigrationEngine
from gui.main_window import MainWindow
from gui.log_panel import GUILogHandler

# Configure logging without file handler initially
# File logging will only be enabled when operations are performed
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
log_filename = None  # Will be set when operation starts

def enable_file_logging():
    """Enable file logging when an operation starts"""
    global log_filename
    if log_filename is None:  # Only create once per session
        log_filename = f"nxmigratorpro_{datetime.now().strftime('%d%m%Y_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
        logging.getLogger().addHandler(file_handler)
        logger.info("="*60)
        logger.info(f"NX Migrator Pro v{__version__} - Operation Log")
        logger.info(f"Log file: {log_filename}")
        logger.info("="*60)
    return log_filename

def main():
    """Entry point for the application"""
    logger.info("Starting NX Migrator Pro v{__version__}")

    # Create root window
    root = ttk.Window(
        title=f"NX Migrator Pro v{__version__}",
        themename="darkly",
        resizable=(True, True)
    )

    # Get screen dimensions
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()

    # Adaptive sizing based on screen resolution
    # For 1080p: Use 1400x950 minimum to show partition summary labels
    # For 4K: Use 80% of screen with higher maximum (e.g., 1400px for 4K displays)
    # This ensures content isn't cut off on high-res displays
    window_width = min(int(screen_width * 0.80), 1600)

    # Set maximum height based on screen resolution
    # 4K and above: Allow up to 1400px tall
    # Below 4K: Cap at 1200px
    max_height = 1400 if screen_height >= 2160 else 1200
    window_height = max(950, min(int(screen_height * 0.80), max_height))

    root.geometry(f"{window_width}x{window_height}")

    # Center window on screen
    root.place_window_center()

    # Create main application window
    app = MainWindow(root)

    # Add GUI log handler to send logs to the log panel
    gui_handler = GUILogHandler(app.log_panel)
    gui_handler.setLevel(logging.DEBUG)
    gui_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
    logging.getLogger().addHandler(gui_handler)

    logger.info("GUI log handler attached")

    # Run application
    root.mainloop()

if __name__ == "__main__":
    # Check if running as administrator on Windows
    if sys.platform == 'win32':
        import ctypes
        if not ctypes.windll.shell32.IsUserAnAdmin():
            messagebox.showerror(
                "Administrator Required",
                "This application requires administrator privileges to access raw disk devices.\n\n"
                "Please run as Administrator."
            )
            sys.exit(1)

    main()
