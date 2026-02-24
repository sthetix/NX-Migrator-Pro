"""
Main GUI Window for SD Card Migrator
"""

import ttkbootstrap as ttk
from ttkbootstrap.constants import *
import threading
from tkinter import messagebox
import webbrowser
import os
import subprocess
import sys
import json
import logging

from gui.disk_selector import DiskSelectorFrame
from gui.partition_viewer import PartitionViewerFrame
from gui.migration_options import MigrationOptionsFrame
from gui.progress_panel import ProgressPanel
from gui.log_panel import LogPanel
from core.disk_manager import DiskManager
from core.partition_scanner import PartitionScanner
from core.migration_engine import MigrationEngine
from core.cleanup_engine import CleanupEngine

def get_sd_tier(size_gb: float) -> int:
    """
    Determine the SD card capacity tier based on size in GB.
    Returns the standard tier (32, 64, 128, 256, 512, 1024, 1536).
    This allows same-tier cards to use Clone Mode.
    """
    # Standard SD card tiers in GB
    tiers = [32, 64, 128, 256, 512, 1024, 1536, 2048]

    for tier in tiers:
        # Allow some tolerance (cards are usually slightly smaller than advertised)
        # e.g., a "128GB" card might report as 119GB
        if size_gb < tier * 0.98:  # Within 2% below the tier
            return tier

    # For very large cards, return the size rounded to nearest tier
    return tiers[-1]


class MainWindow:
    """Main application window"""

    def __init__(self, root):
        self.root = root
        self.disk_manager = DiskManager()
        self.scanner = PartitionScanner()
        self.migration_engine = None

        # State
        self.current_mode = "migration"  # "migration" or "cleanup"
        self.source_disk = None
        self.target_disk = None
        self.source_layout = None
        self.target_layout = None
        self.migration_options = {
            'migrate_fat32': True,
            'migrate_linux': True,
            'migrate_android': True,
            'migrate_emummc': True,
            'expand_fat32': True
        }
        self.cleanup_options = {
            'remove_linux': False,
            'remove_android': False,
            'remove_emummc': False,
            'expand_fat32': True
        }

        # Settings
        self.temp_backup_dir = None  # None = use system default (temp folder)
        self._load_settings()

        # Build UI
        self._create_menu()
        self._create_widgets()
        self._layout_widgets()

        # Bind keyboard shortcut for log toggle (Ctrl+L)
        self.root.bind('<Control-l>', lambda e: self._toggle_log_panel())

        # Load preferences and restore log panel state
        self._load_log_preference()

    def _create_menu(self):
        """Create menu bar"""
        menubar = ttk.Menu(self.root)
        self.root.config(menu=menubar)

        # Settings Menu
        settings_menu = ttk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Settings", menu=settings_menu)
        settings_menu.add_command(label="Temp Backup Directory", command=self._show_backup_dir_settings)

        # Help Menu
        help_menu = ttk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="Usage Guide", command=self._show_usage_guide)
        help_menu.add_command(label="Troubleshooting", command=self._show_troubleshooting)
        help_menu.add_separator()
        help_menu.add_command(label="View Logs", command=self._open_logs)
        help_menu.add_command(label="Report Issue on GitHub", command=self._open_github_issues)
        help_menu.add_separator()
        help_menu.add_command(label="About", command=self._show_about)

    def _create_widgets(self):
        """Create all GUI widgets"""

        # ===== Header =====
        self.header_frame = ttk.Frame(self.root, bootstyle=PRIMARY)

        self.title_label = ttk.Label(
            self.header_frame,
            text="⚙️ NX Migrator Pro",
            font=("Segoe UI", 20, "bold"),
            bootstyle="inverse-primary"
        )

        self.subtitle_label = ttk.Label(
            self.header_frame,
            text="Professional partition management for Nintendo Switch SD cards • Migration • Cleanup • FAT32 • Linux • Android • emuMMC",
            font=("Segoe UI", 10),
            bootstyle="inverse-primary"
        )

        # ===== Mode Selector =====
        self.mode_frame = ttk.Frame(self.root)

        ttk.Label(
            self.mode_frame,
            text="Mode:",
            font=("Segoe UI", 11, "bold")
        ).pack(side=LEFT, padx=(10, 5))

        self.migration_mode_btn = ttk.Button(
            self.mode_frame,
            text="🔄 Migration Mode",
            command=lambda: self._switch_mode("migration"),
            bootstyle="primary",
            width=20
        )
        self.migration_mode_btn.pack(side=LEFT, padx=5)

        self.cleanup_mode_btn = ttk.Button(
            self.mode_frame,
            text="🧹 Cleanup Mode",
            command=lambda: self._switch_mode("cleanup"),
            bootstyle="secondary-outline",
            width=20
        )
        self.cleanup_mode_btn.pack(side=LEFT, padx=5)

        ttk.Label(
            self.mode_frame,
            text="Migration: Copy from small SD to large SD  |  Cleanup: Remove partitions from single SD",
            font=("Segoe UI", 9),
            foreground="gray"
        ).pack(side=LEFT, padx=20)

        # ===== Main Content Area =====
        self.content_frame = ttk.Frame(self.root)

        # Left Panel - Disk Selection
        self.left_panel = ttk.Labelframe(
            self.content_frame,
            text="Step 1: Select Disks",
            bootstyle=INFO,
            padding=10
        )

        self.disk_selector = DiskSelectorFrame(
            self.left_panel,
            self.disk_manager,
            on_source_selected=self._on_source_selected,
            on_target_selected=self._on_target_selected,
            main_window=self
        )

        # Scan button
        self.scan_button = ttk.Button(
            self.left_panel,
            text="🔍 Simulate Migration",
            command=self._scan_sd_cards,
            bootstyle=SUCCESS,
            width=30
        )

        # Middle Panel - Partition Information
        self.middle_panel = ttk.Labelframe(
            self.content_frame,
            text="Step 2: Review Partitions",
            bootstyle=INFO,
            padding=10
        )

        # Source partition view (no tabs, just direct frames)
        self.source_partition_frame = PartitionViewerFrame(
            self.middle_panel,
            title="📀 Source SD Card"
        )

        # Target partition view
        self.target_partition_frame = PartitionViewerFrame(
            self.middle_panel,
            title="💾 Target SD Card (After Migration)"
        )

        # Right Panel - Migration Options
        self.right_panel = ttk.Labelframe(
            self.content_frame,
            text="Step 3: Migration Options",
            bootstyle=INFO,
            padding=10
        )

        self.migration_options_frame = MigrationOptionsFrame(
            self.right_panel,
            on_options_changed=self._on_options_changed
        )

        # Migration button
        self.migrate_button = ttk.Button(
            self.right_panel,
            text="🚀 Start Migration",
            command=self._start_migration,
            bootstyle=SUCCESS,
            width=30,
            state=DISABLED
        )

        # ===== Bottom Panel - Progress =====
        self.bottom_frame = ttk.Frame(self.root)

        # Progress panel
        self.progress_panel = ProgressPanel(self.bottom_frame)

        # ===== Log Panel =====
        self.log_panel = LogPanel(self.root)

        # ===== Status Bar =====
        self.status_frame = ttk.Frame(self.root, bootstyle=DARK)

        self.status_label = ttk.Label(
            self.status_frame,
            text="Ready. Click 'Refresh Disks', select source and target drives, then click 'Simulate Migration'.",
            font=("Segoe UI", 9),
            foreground="white",
            bootstyle="inverse-dark"
        )

        # Log toggle button
        self.log_toggle_btn = ttk.Button(
            self.status_frame,
            text="Show Log",
            command=self._toggle_log_panel,
            bootstyle="info-outline",
            width=12
        )

    def _layout_widgets(self):
        """Layout all widgets"""

        # Header
        self.header_frame.pack(fill=X, pady=(0, 5))
        self.title_label.pack(pady=(10, 3))
        self.subtitle_label.pack(pady=(0, 10))

        # Mode selector
        self.mode_frame.pack(fill=X, pady=(5, 5))

        # Content area
        self.content_frame.pack(fill=BOTH, expand=YES, padx=8, pady=3)

        # Three column layout
        self.left_panel.pack(side=LEFT, fill=BOTH, expand=NO, padx=(0, 5))
        self.middle_panel.pack(side=LEFT, fill=BOTH, expand=YES, padx=5)
        self.right_panel.pack(side=LEFT, fill=BOTH, expand=NO, padx=(5, 0))

        # Left panel content
        self.disk_selector.pack(fill=BOTH, expand=YES)
        self.scan_button.pack(pady=(10, 0))

        # Middle panel content - use grid for perfect 50/50 split
        self.middle_panel.grid_rowconfigure(0, weight=1)  # Source gets 50%
        self.middle_panel.grid_rowconfigure(1, weight=0)  # Separator
        self.middle_panel.grid_rowconfigure(2, weight=1)  # Target gets 50%
        self.middle_panel.grid_columnconfigure(0, weight=1)

        self.source_partition_frame.grid(row=0, column=0, sticky='nsew', pady=(0, 2.5))

        # Separator line for visual clarity
        separator = ttk.Separator(self.middle_panel, orient='horizontal')
        separator.grid(row=1, column=0, sticky='ew', pady=2.5)

        self.target_partition_frame.grid(row=2, column=0, sticky='nsew', pady=(2.5, 0))

        # Right panel content
        self.migration_options_frame.pack(fill=BOTH, expand=YES)
        self.migrate_button.pack(pady=(10, 0))

        # Bottom panel
        self.bottom_frame.pack(fill=X, padx=8, pady=5)
        self.progress_panel.pack(fill=X)

        # Log panel (initially hidden, will be shown/hidden by toggle)
        # Note: pack() is called in log_panel.show() method

        # Status bar
        self.status_frame.pack(fill=X, side=BOTTOM)
        self.status_label.pack(side=LEFT, pady=5, padx=10)
        self.log_toggle_btn.pack(side=RIGHT, pady=5, padx=10)

    def _switch_mode(self, mode):
        """Switch between migration and cleanup modes"""
        if self.current_mode == mode:
            return  # Already in this mode

        self.current_mode = mode

        # Update button styles
        if mode == "migration":
            self.migration_mode_btn.config(bootstyle="primary")
            self.cleanup_mode_btn.config(bootstyle="secondary-outline")

            # Update UI labels for migration mode
            self.left_panel.config(text="Step 1: Select Source & Target Disks")
            self.middle_panel.config(text="Step 2: Review Partitions")
            self.right_panel.config(text="Step 3: Migration Options")
            self.scan_button.config(text="🔍 Simulate Migration")
            self.migrate_button.config(text="🚀 Start Migration")
            self.source_partition_frame.update_title("📀 Source SD Card")
            self.target_partition_frame.update_title("💾 Target SD Card (After Migration)")
            self._update_status("Migration Mode: Select source and target SD cards, then click 'Simulate Migration'.")

            # Show target disk selector
            self.disk_selector.show_target_selector()

            # Set options frame to migration mode
            self.migration_options_frame.set_mode("migration")

        else:  # cleanup mode
            self.migration_mode_btn.config(bootstyle="secondary-outline")
            self.cleanup_mode_btn.config(bootstyle="success")

            # Update UI labels for cleanup mode
            self.left_panel.config(text="Step 1: Select SD Card")
            self.middle_panel.config(text="Step 2: Review Current Partitions")
            self.right_panel.config(text="Step 3: Cleanup Options")
            self.scan_button.config(text="🔍 Scan SD Card")
            self.migrate_button.config(text="🧹 Start Cleanup")
            self.source_partition_frame.update_title("📀 Current SD Card Layout")
            self.target_partition_frame.update_title("✨ After Cleanup (Preview)")
            self._update_status("Cleanup Mode: Select an SD card to clean up unwanted partitions.")

            # Hide target disk selector in cleanup mode
            self.disk_selector.hide_target_selector()

            # Set options frame to cleanup mode
            self.migration_options_frame.set_mode("cleanup")

        # Reset state
        self.source_disk = None
        self.target_disk = None
        self.source_layout = None
        self.target_layout = None
        self.source_partition_frame.clear()
        self.target_partition_frame.clear()
        self.migrate_button.config(state=DISABLED)
        self.disk_selector.clear_selections()

        # Reset progress panel with current mode
        self.progress_panel.reset(mode)

    def _on_source_selected(self, disk_info):
        """Called when source disk is selected"""
        self.source_disk = disk_info
        self.source_layout = None
        self.source_partition_frame.clear()
        self.target_partition_frame.clear()
        self.migrate_button.config(state=DISABLED)

        self._update_status(f"Source selected: {disk_info['letter']} - {disk_info['name']} ({disk_info['size_gb']:.1f} GB)")

    def _on_target_selected(self, disk_info):
        """Called when target disk is selected"""
        self.target_disk = disk_info
        self.target_layout = None
        self.target_partition_frame.clear()
        self.migrate_button.config(state=DISABLED)

        if self.source_disk:
            source_tier = get_sd_tier(self.source_disk['size_gb'])
            target_tier = get_sd_tier(disk_info['size_gb'])

            # Target must be same tier or larger
            if target_tier < source_tier:
                self.show_custom_info(
                    "Invalid Target",
                    f"Target disk ({disk_info['letter']}, {disk_info['size_gb']:.1f} GB) is smaller than source disk ({self.source_disk['letter']}, {self.source_disk['size_gb']:.1f} GB).\n\n"
                    f"Target must be same capacity tier ({source_tier} GB) or larger.",
                    width=500,
                    height=220
                )
                self.disk_selector.clear_target()
                self.target_disk = None
                return

            # Check if same tier (Clone Mode)
            if target_tier == source_tier:
                # Same tier - use Clone Mode (no FAT32 expansion)
                # But target must still have enough bytes for partitions
                if disk_info['size_bytes'] < self.source_disk['size_bytes']:
                    # Target is slightly smaller - warn but allow
                    size_diff_mb = (self.source_disk['size_bytes'] - disk_info['size_bytes']) / (1024 * 1024)
                    self._update_status(f"Target selected: {disk_info['letter']} ({disk_info['size_gb']:.1f} GB) - Clone Mode (target is {size_diff_mb:.0f} MB smaller)")
                else:
                    self._update_status(f"Target selected: {disk_info['letter']} ({disk_info['size_gb']:.1f} GB) - Clone Mode (same capacity tier)")
            else:
                # Larger tier - normal mode with FAT32 expansion
                self._update_status(f"Target selected: {disk_info['letter']} - {disk_info['name']} ({disk_info['size_gb']:.1f} GB)")
        else:
            self._update_status(f"Target selected: {disk_info['letter']} - {disk_info['name']} ({disk_info['size_gb']:.1f} GB)")

    def _on_options_changed(self, options):
        """Called when migration/cleanup options change"""
        if self.current_mode == "migration":
            self.migration_options = options
        else:  # cleanup mode
            # Convert options to cleanup options format
            # In cleanup mode, checked = remove
            self.cleanup_options = {
                'remove_linux': options['migrate_linux'],  # Note: inverted meaning
                'remove_android': options['migrate_android'],
                'remove_emummc': options['migrate_emummc'],
                'expand_fat32': options['expand_fat32']
            }

        # Recalculate layout if we already have source layout
        if self.current_mode == "migration":
            if self.source_layout and self.target_disk:
                self._calculate_layout()
        else:  # cleanup mode
            if self.source_layout:
                self._calculate_layout()

    def _scan_sd_cards(self):
        """Scan SD card and simulate layout (works for both migration and cleanup modes)"""
        if not self.source_disk:
            self.show_custom_info("No Disk Selected", "Please select an SD card first.", width=450, height=200)
            return

        # In migration mode, require target disk
        if self.current_mode == "migration":
            if not self.target_disk:
                self.show_custom_info("No Target Disk", "Please select both source and target SD cards.", width=450, height=200)
                return

        if self.current_mode == "migration":
            self._update_status("Scanning source disk and simulating migration...")
            self.scan_button.config(state=DISABLED, text="⏳ Simulating...")
        else:  # cleanup mode
            self._update_status("Scanning SD card and simulating cleanup...")
            self.scan_button.config(state=DISABLED, text="⏳ Scanning...")

        # Run scan in thread to avoid blocking UI
        def scan_thread():
            try:
                # Scan source disk
                source_layout = self.scanner.scan_disk(self.source_disk['path'])

                # Update UI in main thread
                self.root.after(0, self._on_scan_complete, source_layout, None)

            except Exception as e:
                self.root.after(0, self._on_scan_error, str(e))

        threading.Thread(target=scan_thread, daemon=True).start()

    def _on_scan_complete(self, source_layout, target_layout=None):
        """Called when disk scan completes"""
        self.source_layout = source_layout

        # Update button text based on mode
        if self.current_mode == "migration":
            self.scan_button.config(state=NORMAL, text="🔍 Simulate Migration")
        else:
            self.scan_button.config(state=NORMAL, text="🔍 Scan SD Card")

        # Display source partition information
        self.source_partition_frame.display_layout(source_layout, self.source_disk)

        # Update available toggles based on what partitions exist on the source SD card
        # This applies to both migration and cleanup modes
        self.migration_options_frame.update_available_partitions(
            has_linux=source_layout.has_linux,
            has_android=source_layout.has_android,
            has_emummc=source_layout.has_emummc
        )

        # Sync the options from the frame to ensure we use the correct state
        if self.current_mode == "migration":
            self.migration_options = self.migration_options_frame.options.copy()
        else:
            # Convert to cleanup options format
            options = self.migration_options_frame.options
            self.cleanup_options = {
                'remove_linux': options['migrate_linux'],
                'remove_android': options['migrate_android'],
                'remove_emummc': options['migrate_emummc'],
                'expand_fat32': options['expand_fat32']
            }

        # Update status
        summary = source_layout.get_summary()

        if self.current_mode == "migration":
            self._update_status(f"Scan complete: {summary}. Calculating target layout...")
        else:
            self._update_status(f"Scan complete: {summary}. Select cleanup options and calculate preview...")

        # Automatically calculate and display the simulated target layout
        self._calculate_layout()

    def _on_scan_error(self, error_msg):
        """Called when disk scan fails"""
        self.scan_button.config(state=NORMAL, text="🔍 Simulate Migration")

        self.show_custom_info(
            "Scan Failed",
            f"Failed to scan disks:\n\n{error_msg}",
            width=500,
            height=250
        )

        self._update_status("Scan failed. Please try again.")

    def _calculate_layout(self):
        """Calculate new partition layout (for both migration and cleanup modes)"""
        if not self.source_layout:
            self.show_custom_info(
                "Missing Information",
                "Please scan the SD card first.",
                width=500,
                height=200
            )
            return

        # In migration mode, require target disk
        if self.current_mode == "migration" and not self.target_disk:
            self.show_custom_info(
                "Missing Information",
                "Please select target disk first.",
                width=500,
                height=200
            )
            return

        try:
            self._update_status("Calculating new partition layout...")

            if self.current_mode == "migration":
                # Check if Clone Mode (same capacity tier)
                source_tier = get_sd_tier(self.source_disk['size_gb'])
                target_tier = get_sd_tier(self.target_disk['size_gb'])
                is_clone_mode = (source_tier == target_tier)

                # In Clone Mode, override expand_fat32 to False
                options_for_calc = self.migration_options.copy()
                if is_clone_mode:
                    options_for_calc['expand_fat32'] = False

                # Migration mode: calculate layout for target disk
                new_layout = self.scanner.calculate_target_layout(
                    self.source_layout,
                    self.target_disk['size_bytes'],
                    options_for_calc
                )

                self.target_layout = new_layout

                # Display new layout
                self.target_partition_frame.display_layout(new_layout, self.target_disk)

            else:  # cleanup mode
                # Cleanup mode: calculate layout for same disk (with partitions removed)
                # Use cleanup options to determine what to remove
                cleanup_options_for_calc = {
                    'migrate_fat32': True,  # Always keep FAT32
                    'migrate_linux': not self.cleanup_options['remove_linux'],
                    'migrate_android': not self.cleanup_options['remove_android'],
                    'migrate_emummc': not self.cleanup_options['remove_emummc'],
                    'expand_fat32': self.cleanup_options['expand_fat32']
                }

                new_layout = self.scanner.calculate_target_layout(
                    self.source_layout,
                    self.source_disk['size_bytes'],  # Same disk size
                    cleanup_options_for_calc
                )

                self.target_layout = new_layout

                # Display new layout (use source disk info since it's the same disk)
                self.target_partition_frame.display_layout(new_layout, self.source_disk)

            # Show comparison
            self._show_layout_comparison()

            # Enable action button
            self.migrate_button.config(state=NORMAL)

            if self.current_mode == "migration":
                self._update_status("Layout calculated. Ready to migrate.")
            else:
                self._update_status("Cleanup preview ready. Ready to start cleanup.")

        except Exception as e:
            self.show_custom_info(
                "Calculation Failed",
                f"Failed to calculate new layout:\n\n{str(e)}",
                width=500,
                height=250
            )
            self._update_status("Layout calculation failed.")

    def _show_layout_comparison(self):
        """Show comparison between source and target layouts"""
        if not self.source_layout or not self.target_layout:
            return

        # Build comparison message based on mode
        if self.current_mode == "migration":
            msg = "Migration Summary:\n\n"

            # FAT32
            if self.migration_options['migrate_fat32']:
                src_fat = self.source_layout.get_fat32_size_mb()
                dst_fat = self.target_layout.get_fat32_size_mb()
                fat32_gain = dst_fat - src_fat
                if self.migration_options['expand_fat32']:
                    msg += f"✓ FAT32: {src_fat:,} MB → {dst_fat:,} MB (+{fat32_gain:,} MB gained)\n"
                else:
                    msg += f"✓ FAT32: {src_fat:,} MB → {dst_fat:,} MB (no expansion)\n"

            # Linux
            if self.source_layout.has_linux and self.migration_options['migrate_linux']:
                linux_size = self.source_layout.get_linux_size_mb()
                msg += f"✓ Linux: {linux_size:,} MB (preserved)\n"

            # Android
            if self.source_layout.has_android and self.migration_options['migrate_android']:
                android_size = self.source_layout.get_android_size_mb()
                android_type = "Dynamic" if self.source_layout.android_dynamic else "Legacy"
                msg += f"✓ Android ({android_type}): {android_size:,} MB (preserved)\n"

            # emuMMC
            if self.source_layout.has_emummc and self.migration_options['migrate_emummc']:
                emummc_size = self.source_layout.get_emummc_size_mb()
                emummc_type = "Dual" if self.source_layout.emummc_double else "Single"
                msg += f"✓ emuMMC ({emummc_type}): {emummc_size:,} MB (preserved)\n"

            msg += f"\nSource Disk: {self.source_disk['size_gb']:.1f} GB\n"
            msg += f"Target Disk: {self.target_disk['size_gb']:.1f} GB"

            self.show_custom_info("Layout Comparison", msg, width=550, height=400)

        else:  # cleanup mode
            msg = "Cleanup Summary:\n\n"

            # FAT32
            src_fat = self.source_layout.get_fat32_size_mb()
            dst_fat = self.target_layout.get_fat32_size_mb()
            fat32_gain = dst_fat - src_fat
            if self.cleanup_options['expand_fat32']:
                msg += f"✓ FAT32: {src_fat:,} MB → {dst_fat:,} MB (+{fat32_gain:,} MB reclaimed)\n"
            else:
                msg += f"✓ FAT32: {src_fat:,} MB (no expansion)\n"

            # Linux
            if self.source_layout.has_linux:
                linux_size = self.source_layout.get_linux_size_mb()
                if self.cleanup_options['remove_linux']:
                    msg += f"✗ Linux: {linux_size:,} MB (will be REMOVED)\n"
                else:
                    msg += f"✓ Linux: {linux_size:,} MB (preserved)\n"

            # Android
            if self.source_layout.has_android:
                android_size = self.source_layout.get_android_size_mb()
                android_type = "Dynamic" if self.source_layout.android_dynamic else "Legacy"
                if self.cleanup_options['remove_android']:
                    msg += f"✗ Android ({android_type}): {android_size:,} MB (will be REMOVED)\n"
                else:
                    msg += f"✓ Android ({android_type}): {android_size:,} MB (preserved)\n"

            # emuMMC
            if self.source_layout.has_emummc:
                emummc_size = self.source_layout.get_emummc_size_mb()
                emummc_type = "Dual" if self.source_layout.emummc_double else "Single"
                if self.cleanup_options['remove_emummc']:
                    msg += f"✗ emuMMC ({emummc_type}): {emummc_size:,} MB (will be REMOVED)\n"
                else:
                    msg += f"✓ emuMMC ({emummc_type}): {emummc_size:,} MB (preserved)\n"

            msg += f"\nSD Card: {self.source_disk['size_gb']:.1f} GB"

            self.show_custom_info("Cleanup Summary", msg, width=550, height=380)

    def _get_source_file_info(self):
        """Get file count and total size from source FAT32 partition"""
        import subprocess
        try:
            # Find source FAT32 drive letter
            source_fat32_part = None
            for part in self.source_layout.partitions:
                if part.category == 'FAT32':
                    source_fat32_part = part
                    break

            if not source_fat32_part:
                return None, None

            # Get drive letter for source partition
            from core.migration_engine import MigrationEngine
            temp_engine = MigrationEngine(None, None, None, None, None)
            source_drive = temp_engine._get_drive_letter_for_partition(
                self.source_disk['path'],
                source_fat32_part.start_sector
            )

            if not source_drive:
                return None, None

            # Count files and get total size
            count_cmd = f'''
            $files = Get-ChildItem -Path "{source_drive}" -Recurse -File -Force -ErrorAction SilentlyContinue
            $count = ($files | Measure-Object).Count
            $size = ($files | Measure-Object -Property Length -Sum).Sum
            if ($size -eq $null) {{ $size = 0 }}
            Write-Output "$count|$size"
            '''

            result = subprocess.run(
                ['powershell', '-NoProfile', '-WindowStyle', 'Hidden', '-Command', count_cmd],
                capture_output=True,
                text=True,
                timeout=120,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
            )

            if result.returncode == 0 and result.stdout.strip():
                parts = result.stdout.strip().split('|')
                if len(parts) == 2:
                    file_count = int(parts[0]) if parts[0].isdigit() else 0
                    total_bytes = int(parts[1]) if parts[1].isdigit() else 0
                    return file_count, total_bytes

        except Exception as e:
            logging.getLogger(__name__).warning(f"Could not get source file info: {e}")

        return None, None

    def _show_migration_info_popup(self, file_count, total_bytes):
        """Show migration info popup with file count and archive bit warning"""
        total_gb = total_bytes / (1024**3) if total_bytes else 0

        msg = f"📋 MIGRATION INFORMATION\n\n"
        msg += f"Source: {self.source_disk['letter']} - {self.source_disk['name']} ({self.source_disk['size_gb']:.1f} GB)\n"
        msg += f"Target: {self.target_disk['letter']} - {self.target_disk['name']} ({self.target_disk['size_gb']:.1f} GB)\n\n"

        if file_count is not None:
            msg += f"Files to copy: {file_count:,} files\n"
        if total_gb > 0:
            msg += f"Data to copy: {total_gb:.2f} GB\n\n"

        msg += f"⚠️ ARCHIVE BIT WARNING:\n\n"
        msg += f"The automatic Archive bit fix may fail with\n"
        msg += f"large numbers of files. If this happens:\n\n"
        msg += f"Your SD card will still work fine!\n\n"
        msg += f"If needed, use Hekate to fix Archive Bit:\n"
        msg += f"   1. Boot Hekate on your Switch\n"
        msg += f"   2. Go to Tools (top) > Arch bit (bottom)\n"
        msg += f"   3. Select 'Fix Archive Bit'\n"
        msg += f"   4. Select your SD card and run\n\n"
        msg += f"Ready to start migration?"

        response = self.show_custom_confirm(
            "Ready to Migrate",
            msg,
            yes_text="Start Migration",
            no_text="Cancel",
            style="info",
            width=550,
            height=800
        )

        return response

    def _start_migration(self):
        """Start the migration or cleanup process (depending on mode)"""

        if self.current_mode == "migration":
            # Show migration info popup first
            file_count, total_bytes = self._get_source_file_info()
            info_response = self._show_migration_info_popup(file_count, total_bytes)

            if not info_response:
                return

            # Migration mode confirmations
            response = self.show_custom_confirm(
                "Confirm Migration",
                f"⚠️ WARNING ⚠️\n\n"
                f"This will ERASE ALL DATA on the target disk:\n"
                f"{self.target_disk['letter']} - {self.target_disk['name']} ({self.target_disk['size_gb']:.1f} GB)\n\n"
                f"Source disk ({self.source_disk['letter']}) will NOT be modified.\n\n"
                f"Are you sure you want to continue?",
                yes_text="Yes, Continue",
                no_text="Cancel",
                style="warning",
                width=550,
                height=400
            )

            if not response:
                return

            # Double confirmation
            response2 = self.show_custom_confirm(
                "Final Confirmation",
                f"⚠️ LAST WARNING ⚠️\n\n"
                f"All data on {self.target_disk['letter']} ({self.target_disk['name']}) will be PERMANENTLY ERASED.\n\n"
                f"This action cannot be undone!",
                yes_text="Yes, ERASE and Migrate",
                no_text="Cancel",
                style="danger",
                width=550,
                height=330
            )

            if not response2:
                return

            # Enable file logging for this operation
            from main import enable_file_logging
            log_file = enable_file_logging()
            logging.getLogger(__name__).info(f"Migration operation started - logging to {log_file}")

            # Disable UI during migration
            self._set_ui_enabled(False)

            # Create migration engine
            self.migration_engine = MigrationEngine(
                self.source_disk,
                self.target_disk,
                self.source_layout,
                self.target_layout,
                self.migration_options
            )

            # Connect progress callbacks
            self.migration_engine.on_progress = self._on_operation_progress
            self.migration_engine.on_complete = self._on_operation_complete
            self.migration_engine.on_error = self._on_operation_error

            # Start migration in thread
            self._update_status("Migration in progress...")
            self.progress_panel.start()

            threading.Thread(
                target=self.migration_engine.run,
                daemon=True
            ).start()

        else:  # cleanup mode
            # Cleanup mode confirmations
            removed_parts = []
            if self.cleanup_options['remove_linux'] and self.source_layout.has_linux:
                removed_parts.append("Linux partition")
            if self.cleanup_options['remove_android'] and self.source_layout.has_android:
                removed_parts.append("Android partitions")
            if self.cleanup_options['remove_emummc'] and self.source_layout.has_emummc:
                removed_parts.append("emuMMC partition(s)")

            if removed_parts:
                parts_str = ", ".join(removed_parts)
            else:
                parts_str = "No partitions will be removed (only FAT32 expansion)"

            response = self.show_custom_confirm(
                "Confirm Cleanup",
                f"⚠️ WARNING ⚠️\n\n"
                f"This will MODIFY the disk:\n"
                f"{self.source_disk['letter']} - {self.source_disk['name']} ({self.source_disk['size_gb']:.1f} GB)\n\n"
                f"Partitions to remove:\n{parts_str}\n\n"
                f"FAT32 data will be backed up temporarily, then restored.\n\n"
                f"⚠️ IMPORTANT: Make sure you have a backup of your SD card!\n\n"
                f"Are you sure you want to continue?",
                yes_text="Yes, Continue",
                no_text="Cancel",
                style="warning",
                width=600,
                height=500
            )

            if not response:
                return

            # Double confirmation
            response2 = self.show_custom_confirm(
                "Final Confirmation",
                f"⚠️ LAST WARNING ⚠️\n\n"
                f"The disk {self.source_disk['letter']} will be modified.\n"
                f"Removed partitions will be PERMANENTLY DELETED.\n\n"
                f"This action cannot be undone!\n\n"
                f"Do you have a backup?",
                yes_text="Yes, I have a backup - Proceed",
                no_text="Cancel",
                style="danger",
                width=550,
                height=400
            )

            if not response2:
                return

            # Enable file logging for this operation
            from main import enable_file_logging
            log_file = enable_file_logging()
            logging.getLogger(__name__).info(f"Cleanup operation started - logging to {log_file}")

            # Disable UI during cleanup
            self._set_ui_enabled(False)

            # Create cleanup engine with temp backup directory setting
            self.cleanup_engine = CleanupEngine(
                self.source_disk,
                self.source_layout,
                self.target_layout,
                self.cleanup_options,
                temp_backup_dir=self.temp_backup_dir
            )

            # Connect progress callbacks
            self.cleanup_engine.on_progress = self._on_operation_progress
            self.cleanup_engine.on_complete = self._on_operation_complete
            self.cleanup_engine.on_error = self._on_operation_error

            # Start cleanup in thread
            self._update_status("Cleanup in progress...")
            self.progress_panel.start()

            threading.Thread(
                target=self.cleanup_engine.run,
                daemon=True
            ).start()

    def _on_operation_progress(self, stage, percent, message):
        """Called during operation progress (migration or cleanup)"""
        # Show stage and percent in progress panel (top)
        self.root.after(0, self.progress_panel.update, stage, percent)
        # Show detailed message in status bar (bottom)
        status_message = f"{stage} - {message}"
        self.root.after(0, self._update_status, status_message)

    def _on_operation_complete(self):
        """Called when operation completes successfully (migration or cleanup)"""
        def complete_ui():
            self.progress_panel.complete()
            self._set_ui_enabled(True)

            if self.current_mode == "migration":
                self._update_status("Migration completed successfully!")
                self.show_custom_info(
                    "Migration Complete",
                    "✓ SD card migration completed successfully!\n\n"
                    "You can now safely remove both SD cards.",
                    width=500,
                    height=220
                )
            else:  # cleanup mode
                self._update_status("Cleanup completed successfully!")
                self.show_custom_info(
                    "Cleanup Complete",
                    "✓ SD card cleanup completed successfully!\n\n"
                    "Unwanted partitions have been removed and FAT32 has been expanded.\n\n"
                    "You can now safely remove the SD card.",
                    width=550,
                    height=300
                )

        self.root.after(0, complete_ui)

    def _on_operation_error(self, error_msg):
        """Called when operation fails (migration or cleanup)"""
        def error_ui():
            self.progress_panel.error()
            self._set_ui_enabled(True)

            if self.current_mode == "migration":
                self._update_status(f"Migration failed: {error_msg}")
                self.show_custom_info(
                    "Migration Failed",
                    f"Migration failed with error:\n\n{error_msg}\n\n"
                    f"The target disk may be in an inconsistent state.",
                    width=550,
                    height=280
                )
            else:  # cleanup mode
                self._update_status(f"Cleanup failed: {error_msg}")
                self.show_custom_info(
                    "Cleanup Failed",
                    f"Cleanup failed with error:\n\n{error_msg}\n\n"
                    f"The SD card may be in an inconsistent state.\n"
                    f"Please restore from backup if needed.",
                    width=550,
                    height=300
                )

        self.root.after(0, error_ui)

    def _set_ui_enabled(self, enabled):
        """Enable/disable UI during migration"""
        state = NORMAL if enabled else DISABLED

        self.disk_selector.set_enabled(enabled)
        self.scan_button.config(state=state)
        self.migrate_button.config(state=state)
        self.migration_options_frame.set_enabled(enabled)

    def _update_status(self, message):
        """Update status bar message"""
        self.status_label.config(text=message)

    def _toggle_log_panel(self):
        """Toggle log panel visibility"""
        self.log_panel.toggle()

        # Update button text
        if self.log_panel.is_visible():
            self.log_toggle_btn.config(text="Hide Log")
            self._save_log_preference(True)
        else:
            self.log_toggle_btn.config(text="Show Log")
            self._save_log_preference(False)

    def center_window(self, window):
        """Center a popup window on the main window"""
        # This function is now a wrapper to call the actual centering logic
        # after a small delay, preventing the "flicker" effect.
        window.after(10, lambda: self._do_center(window))

    def _do_center(self, window):
        """Actually center the window"""
        # Update both parent and child window to get accurate current positions
        self.root.update_idletasks()
        window.update_idletasks()

        parent_x = self.root.winfo_x()
        parent_y = self.root.winfo_y()
        parent_w = self.root.winfo_width()
        parent_h = self.root.winfo_height()

        window_w = window.winfo_width()
        window_h = window.winfo_height()

        x = parent_x + (parent_w // 2) - (window_w // 2)
        y = parent_y + (parent_h // 2) - (window_h // 2)

        window.geometry(f"+{x}+{y}")

    def show_custom_info(self, title, message, parent=None, blocking=True, width=400, height=200):
        """Show a custom centered info dialog"""
        # Scale down for 1080p (cosmetic improvement)
        screen_height = self.root.winfo_screenheight()
        if screen_height < 1440:  # 1080p or lower
            width = int(width * 0.75)
            height = int(height * 0.75)

        parent_window = parent if parent else self.root
        dialog = ttk.Toplevel(parent_window)
        dialog.title(title)
        dialog.transient(parent_window)

        # Withdraw the window to prevent it from appearing at default position
        dialog.withdraw()

        dialog.grab_set()

        info_frame = ttk.Frame(dialog, padding=20)
        info_frame.pack(fill=BOTH, expand=True)

        ttk.Label(info_frame, text=message, wraplength=width-60, justify=CENTER).pack(pady=20)

        ttk.Button(info_frame, text="OK", command=dialog.destroy, bootstyle="primary").pack()

        # Update geometry and calculate centered position
        dialog.update_idletasks()

        # Get parent window position
        parent_x = parent_window.winfo_x()
        parent_y = parent_window.winfo_y()
        parent_w = parent_window.winfo_width()
        parent_h = parent_window.winfo_height()

        # Calculate centered position
        x = parent_x + (parent_w // 2) - (width // 2)
        y = parent_y + (parent_h // 2) - (height // 2)

        # Set geometry with position
        dialog.geometry(f"{width}x{height}+{x}+{y}")

        # Now show the window at the correct position
        dialog.deiconify()

        # Force window to front and gain focus (essential for popups from background threads)
        dialog.lift()
        dialog.attributes('-topmost', True)
        dialog.after(100, lambda: dialog.attributes('-topmost', False))
        dialog.focus_force()

        if blocking:
            self.root.wait_window(dialog)

    def show_custom_confirm(self, title, message, yes_text="Yes", no_text="No", style="primary", width=450, height=250):
        """Show a custom centered confirmation dialog that returns True or False."""
        # Scale down for 1080p (cosmetic improvement)
        screen_height = self.root.winfo_screenheight()
        if screen_height < 1440:  # 1080p or lower
            width = int(width * 0.75)
            height = int(height * 0.75)

        dialog = ttk.Toplevel(self.root)
        dialog.title(title)
        dialog.transient(self.root)

        # Withdraw the window to prevent it from appearing at default position
        dialog.withdraw()

        dialog.grab_set()

        result = [False]  # Use a list to allow modification from inner function

        def on_yes():
            result[0] = True
            dialog.destroy()

        def on_no():
            result[0] = False
            dialog.destroy()

        info_frame = ttk.Frame(dialog, padding=20)
        info_frame.pack(fill=BOTH, expand=True)
        ttk.Label(info_frame, text=message, wraplength=width-60, justify=CENTER).pack(pady=20)

        button_frame = ttk.Frame(info_frame)
        button_frame.pack(pady=20)
        ttk.Button(button_frame, text=yes_text, command=on_yes, bootstyle=style).pack(side=LEFT, padx=10)
        ttk.Button(button_frame, text=no_text, command=on_no, bootstyle="secondary").pack(side=LEFT, padx=10)

        # Update geometry and calculate centered position
        dialog.update_idletasks()

        # Get parent window position
        parent_x = self.root.winfo_x()
        parent_y = self.root.winfo_y()
        parent_w = self.root.winfo_width()
        parent_h = self.root.winfo_height()

        # Calculate centered position
        x = parent_x + (parent_w // 2) - (width // 2)
        y = parent_y + (parent_h // 2) - (height // 2)

        # Set geometry with position
        dialog.geometry(f"{width}x{height}+{x}+{y}")

        # Now show the window at the correct position
        dialog.deiconify()

        # Force window to front and gain focus
        dialog.lift()
        dialog.attributes('-topmost', True)
        dialog.after(100, lambda: dialog.attributes('-topmost', False))
        dialog.focus_force()

        self.root.wait_window(dialog)
        return result[0]

    # ===== Menu Handlers =====

    def _show_usage_guide(self):
        """Show usage guide dialog"""
        usage_text = """USAGE GUIDE

Step 1: Select Disks
• Insert both source (smaller) and target (larger) SD cards
• Click "Refresh Disks" to detect SD cards
• Select your Source SD Card (original)
• Select your Target SD Card (destination)

WARNING: Target disk will be COMPLETELY ERASED!

Step 2: Scan Source
• Click "Simulate Migration"
• Wait for the scan to complete
• Review the detected partition layout

The tool automatically detects:
• FAT32 partition (hos_data)
• Linux partition (L4T) if present
• Android partitions (Dynamic or Legacy) if present
• emuMMC partitions (Single or Dual) if present

Step 3: Configure Migration
Choose what to migrate:
• FAT32 Partition (always migrated, auto-expanded)
• Linux Partition (optional)
• Android Partitions (optional)
• emuMMC Partitions (optional)

Step 4: Review Layout
• Review the new partition layout
• Check the comparison showing size changes
• Verify FAT32 expansion and free space

Step 5: Start Migration
• Click "Start Migration"
• Confirm the warning dialogs
• Wait for migration to complete (30-60 min for 128GB)

DO NOT remove SD cards or power off during migration!

Step 6: Verification
• Safely remove both SD cards
• Insert target SD card into Nintendo Switch
• Boot normally - all data and partitions preserved
"""

        self._show_scrollable_dialog("Usage Guide", usage_text, width=700, height=650)

    def _show_troubleshooting(self):
        """Show troubleshooting dialog"""
        troubleshooting_text = """TROUBLESHOOTING

"Administrator Required" Error
• Right-click the executable and select "Run as Administrator"
• Administrator privileges are required for direct disk access

"No SD Cards Found"
• Make sure SD cards are properly inserted
• Click "Refresh Disks" to re-scan
• Try different USB ports
• Check Device Manager for SD card readers
• Ensure SD cards are not mounted/in use by other programs

"Target disk must be larger"
• Ensure target SD card is actually larger than source
• Some SD cards report slightly different sizes
• Try a different target card with more capacity

Migration Fails
• Check SD card connections
• Try a different SD card reader
• Verify target SD card is not write-protected
• Check for bad sectors on target SD card
• Close all programs accessing the SD cards
• Run Check Disk (chkdsk) on the SD cards

emuMMC Not Working After Migration
• The tool automatically updates emuMMC sector offsets
• If issues persist, verify emuMMC/RAW1 or emuMMC/RAW2
  folders contain correct offsets
• Check the log file for emuMMC update errors
• Ensure "Migrate emuMMC" option was enabled

Slow Migration Speed
• Use a high-quality SD card reader (USB 3.0+)
• Avoid USB hubs - connect directly to PC
• Close background programs to free up system resources
• Check if antivirus is scanning the SD cards

Partition Layout Incorrect
• Verify source SD card is correctly set up
• Check log file for partition detection warnings
• Try re-scanning the source disk
• Ensure hekate partition manager was used originally

For more help:
• Check the log file (NXMigrator_YYYYMMDD_HHMMSS.log)
• Report issues on GitHub with log file attached
"""

        self._show_scrollable_dialog("Troubleshooting", troubleshooting_text, width=700, height=650)

    def _open_logs(self):
        """Open the most recent log file"""
        try:
            # Find the most recent log file
            log_files = [f for f in os.listdir('.') if f.startswith('nx_migrator_pro_') and f.endswith('.log')]

            if not log_files:
                self.show_custom_info(
                    "No Logs Found",
                    "No log files found in the current directory.",
                    width=450,
                    height=200
                )
                return

            # Sort by modification time and get the most recent
            log_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            latest_log = log_files[0]

            # Open with default text editor
            if sys.platform == 'win32':
                os.startfile(latest_log)
            elif sys.platform == 'darwin':
                subprocess.run(['open', latest_log])
            else:
                subprocess.run(['xdg-open', latest_log])

        except Exception as e:
            self.show_custom_info(
                "Error Opening Log",
                f"Failed to open log file:\n\n{str(e)}",
                width=500,
                height=220
            )

    def _open_github_issues(self):
        """Open GitHub issues page"""
        try:
            webbrowser.open('https://github.com/sthetix/NANDFixPro/issues')
        except Exception as e:
            self.show_custom_info(
                "Error",
                f"Failed to open browser:\n\n{str(e)}",
                width=450,
                height=250
            )

    def _show_about(self):
        """Show about dialog"""
        # Get version from main module
        try:
            import __main__
            version = getattr(__main__, '__version__', '1.0.5')
        except:
            version = '1.0.5'

        about_text = f"""NX MIGRATOR PRO

Version: {version}

A professional partition management tool for Nintendo Switch SD cards.

Features:
• Migration Mode - Migrate partitions from smaller to larger SD
• Cleanup Mode - Remove unwanted partitions and expand FAT32

Supports: FAT32, Linux (L4T), Android, emuMMC

Copyright (c) 2025 Sthetix
License: GPL-2.0

Made for the Nintendo Switch homebrew community
"""

        self._show_scrollable_dialog("About NX Migrator Pro", about_text, width=600, height=530)

    def _show_scrollable_dialog(self, title, content, width=600, height=500):
        """Show a scrollable text dialog"""
        # Scale down for 1080p (cosmetic improvement)
        screen_height = self.root.winfo_screenheight()
        if screen_height < 1440:  # 1080p or lower
            width = int(width * 0.75)
            height = int(height * 0.75)

        dialog = ttk.Toplevel(self.root)
        dialog.title(title)
        dialog.transient(self.root)

        # Withdraw the window to prevent it from appearing at default position
        dialog.withdraw()

        dialog.grab_set()

        # Create frame for content
        content_frame = ttk.Frame(dialog, padding=10)
        content_frame.pack(fill=BOTH, expand=True)

        # Create text widget with scrollbar
        text_frame = ttk.Frame(content_frame)
        text_frame.pack(fill=BOTH, expand=True, pady=(0, 10))

        scrollbar = ttk.Scrollbar(text_frame)
        scrollbar.pack(side=RIGHT, fill=Y)

        text_widget = ttk.Text(
            text_frame,
            wrap='word',
            yscrollcommand=scrollbar.set,
            font=("Consolas", 9),
            padx=10,
            pady=10,
            height=15
        )
        text_widget.pack(side=LEFT, fill=BOTH, expand=False)
        scrollbar.config(command=text_widget.yview)

        # Insert content
        text_widget.insert('1.0', content)
        text_widget.config(state='disabled')

        # Close button
        ttk.Button(
            content_frame,
            text="Close",
            command=dialog.destroy,
            bootstyle="primary",
            width=15
        ).pack()

        # Update geometry and calculate centered position
        dialog.update_idletasks()

        # Get parent window position
        parent_x = self.root.winfo_x()
        parent_y = self.root.winfo_y()
        parent_w = self.root.winfo_width()
        parent_h = self.root.winfo_height()

        # Calculate centered position
        x = parent_x + (parent_w // 2) - (width // 2)
        y = parent_y + (parent_h // 2) - (height // 2)

        # Set geometry with position
        dialog.geometry(f"{width}x{height}+{x}+{y}")

        # Now show the window at the correct position
        dialog.deiconify()

        # Force window to front
        dialog.lift()
        dialog.attributes('-topmost', True)
        dialog.after(100, lambda: dialog.attributes('-topmost', False))
        dialog.focus_force()

    def _save_log_preference(self, visible):
        """Save log panel visibility preference"""
        try:
            prefs = {'log_panel_visible': visible}
            with open('.nx_migrator_prefs.json', 'w') as f:
                json.dump(prefs, f)
        except Exception:
            # Silently ignore errors saving preferences
            pass

    def _load_log_preference(self):
        """Load and apply log panel visibility preference"""
        try:
            if os.path.exists('.nx_migrator_prefs.json'):
                with open('.nx_migrator_prefs.json', 'r') as f:
                    prefs = json.load(f)
                    if prefs.get('log_panel_visible', False):
                        self.log_panel.show()
                        self.log_toggle_btn.config(text="Hide Log")
        except Exception:
            # Silently ignore errors loading preferences
            pass

    def _load_settings(self):
        """Load application settings"""
        try:
            if os.path.exists('.nx_migrator_prefs.json'):
                with open('.nx_migrator_prefs.json', 'r') as f:
                    prefs = json.load(f)
                    self.temp_backup_dir = prefs.get('temp_backup_dir', None)
        except Exception:
            pass

    def _save_settings(self):
        """Save application settings"""
        try:
            prefs = {}
            # Load existing prefs first to preserve log_panel_visible
            if os.path.exists('.nx_migrator_prefs.json'):
                with open('.nx_migrator_prefs.json', 'r') as f:
                    prefs = json.load(f)

            # Update with current settings
            prefs['temp_backup_dir'] = self.temp_backup_dir

            with open('.nx_migrator_prefs.json', 'w') as f:
                json.dump(prefs, f)
        except Exception as e:
            logging.getLogger(__name__).warning(f"Could not save settings: {e}")

    def _show_backup_dir_settings(self):
        """Show dialog to configure temp backup directory"""
        # Create dialog
        dialog = ttk.Toplevel(self.root)
        dialog.title("Temp Backup Directory Settings")
        dialog.transient(self.root)
        dialog.withdraw()
        dialog.grab_set()

        # Set size
        width = 600
        height = 330

        # Main frame
        main_frame = ttk.Frame(dialog, padding=20)
        main_frame.pack(fill=BOTH, expand=True)

        # Title
        ttk.Label(
            main_frame,
            text="⚙️ Temp Backup Directory",
            font=("Segoe UI", 12, "bold")
        ).pack(pady=(0, 15))

        # Description
        desc_text = (
            "Cleanup mode backs up FAT32 data to a temporary folder before\n"
            "wiping the SD card. By default, the system temp folder is used.\n\n"
            "You can specify a custom location if your C: drive has limited space."
        )
        ttk.Label(main_frame, text=desc_text, justify=CENTER).pack(pady=(0, 15))

        # Path display frame
        path_frame = ttk.Frame(main_frame)
        path_frame.pack(fill=X, pady=(0, 10))

        ttk.Label(path_frame, text="Current location:", font=("Segoe UI", 9)).pack(side=LEFT)

        current_path = self.temp_backup_dir if self.temp_backup_dir else "System default (temp folder)"
        path_label = ttk.Label(path_frame, text=current_path, font=("Segoe UI", 9, "bold"), foreground="#28a745")
        path_label.pack(side=LEFT, padx=(10, 0))

        # Button frame
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(pady=(10, 0))

        def browse_folder():
            from tkinter import filedialog
            folder = filedialog.askdirectory(
                title="Select Temp Backup Directory",
                initialdir=self.temp_backup_dir if self.temp_backup_dir else os.path.expanduser("~")
            )
            if folder:
                self.temp_backup_dir = folder
                path_label.config(text=folder)
                self._save_settings()
                self._update_status(f"Temp backup directory set to: {folder}")

        def reset_to_default():
            self.temp_backup_dir = None
            path_label.config(text="System default (temp folder)")
            self._save_settings()
            self._update_status("Temp backup directory reset to system default")

        ttk.Button(
            button_frame,
            text="📁 Browse...",
            command=browse_folder,
            bootstyle="info",
            width=15
        ).pack(side=LEFT, padx=5)

        ttk.Button(
            button_frame,
            text="↺ Reset to Default",
            command=reset_to_default,
            bootstyle="secondary",
            width=18
        ).pack(side=LEFT, padx=5)

        ttk.Button(
            button_frame,
            text="Close",
            command=dialog.destroy,
            bootstyle="primary",
            width=12
        ).pack(side=LEFT, padx=5)

        # Center dialog
        dialog.update_idletasks()
        parent_x = self.root.winfo_x()
        parent_y = self.root.winfo_y()
        parent_w = self.root.winfo_width()
        parent_h = self.root.winfo_height()
        x = parent_x + (parent_w // 2) - (width // 2)
        y = parent_y + (parent_h // 2) - (height // 2)
        dialog.geometry(f"{width}x{height}+{x}+{y}")
        dialog.deiconify()
        dialog.lift()
        dialog.attributes('-topmost', True)
        dialog.after(100, lambda: dialog.attributes('-topmost', False))
        dialog.focus_force()
